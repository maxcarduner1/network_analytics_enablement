# Databricks notebook source
# MAGIC %md
# MAGIC # Network Analytics Supervisor Agent
# MAGIC
# MAGIC LangGraph supervisor that routes between:
# MAGIC - **Genie** (`01f1434885a51fc4bf4d5fbf5d3fb928`) — answers structured-data questions over `cmegdemos_catalog.network_analytics_enablement` tables
# MAGIC - **Knowledge Assistant** (`ka-62de30a2-endpoint`) — answers FCC BDC methodology / column / interpretation questions
# MAGIC
# MAGIC MLflow tracing tags every trace with `end_user_email`, resolved from (in order):
# MAGIC 1. `custom_inputs.end_user_email` (caller passes it explicitly)
# MAGIC 2. `databricks-request-context` header (when invoked with workspace OBO)
# MAGIC 3. Fallback: `"unknown"`

# COMMAND ----------

# MAGIC %pip install -qqqq -U \
# MAGIC   mlflow[databricks]>=3.0 \
# MAGIC   databricks-agents>=1.0 \
# MAGIC   databricks-langchain \
# MAGIC   langgraph>=0.3 \
# MAGIC   langgraph-supervisor \
# MAGIC   langchain-core
# MAGIC %restart_python

# COMMAND ----------

CATALOG = "cmegdemos_catalog"
SCHEMA = "network_analytics_enablement"
UC_MODEL_NAME = f"{CATALOG}.{SCHEMA}.network_analytics_supervisor"
ENDPOINT_NAME = "network-analytics-supervisor"

GENIE_SPACE_ID = "01f1434885a51fc4bf4d5fbf5d3fb928"
KA_ENDPOINT = "ka-62de30a2-endpoint"
ROUTER_LLM = "databricks-claude-sonnet-4-6"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Agent module
# MAGIC The cell below writes `supervisor_agent.py` to the working directory so MLflow can log it as a code artifact.

# COMMAND ----------

# MAGIC %%writefile supervisor_agent.py
# MAGIC """Network Analytics supervisor agent.
# MAGIC
# MAGIC Routes between a Genie space (structured queries) and a Knowledge Assistant
# MAGIC (BDC methodology) and tags every MLflow trace with the calling user's email.
# MAGIC """
# MAGIC
# MAGIC import os
# MAGIC import json
# MAGIC import uuid
# MAGIC from typing import Any, Generator, Optional
# MAGIC
# MAGIC import mlflow
# MAGIC import mlflow.deployments
# MAGIC from mlflow.entities import SpanType
# MAGIC from mlflow.pyfunc import ChatAgent
# MAGIC from mlflow.types.agent import (
# MAGIC     ChatAgentChunk,
# MAGIC     ChatAgentMessage,
# MAGIC     ChatAgentResponse,
# MAGIC     ChatContext,
# MAGIC )
# MAGIC
# MAGIC from databricks_langchain import ChatDatabricks
# MAGIC from databricks_langchain.genie import GenieAgent
# MAGIC from databricks.sdk import WorkspaceClient
# MAGIC from langchain_core.messages import (
# MAGIC     AIMessage,
# MAGIC     BaseMessage,
# MAGIC     HumanMessage,
# MAGIC     SystemMessage,
# MAGIC     ToolMessage,
# MAGIC )
# MAGIC from langchain_core.tools import tool
# MAGIC from langgraph.graph import END, StateGraph
# MAGIC from langgraph.graph.message import add_messages
# MAGIC from langgraph.prebuilt import ToolNode
# MAGIC from typing_extensions import TypedDict, Annotated
# MAGIC
# MAGIC GENIE_SPACE_ID = os.environ.get("GENIE_SPACE_ID", "01f1434885a51fc4bf4d5fbf5d3fb928")
# MAGIC KA_ENDPOINT = os.environ.get("KA_ENDPOINT", "ka-62de30a2-endpoint")
# MAGIC ROUTER_LLM = os.environ.get("ROUTER_LLM", "databricks-claude-sonnet-4-6")
# MAGIC
# MAGIC SUPERVISOR_SYSTEM = """You are the supervisor for a Network Analytics assistant.
# MAGIC You have two specialist tools:
# MAGIC
# MAGIC 1. `query_network_data` — call when the user asks for numbers, counts, aggregations,
# MAGIC    rows, or anything that requires running SQL over the network analytics tables
# MAGIC    (fcc_bdc_h3_seattle, building_footprints, cell_towers,
# MAGIC    downtown_seattle_building_coverage).
# MAGIC
# MAGIC 2. `lookup_bdc_methodology` — call when the user asks what something means, how a
# MAGIC    value is defined, methodology, FCC filing process, technology codes,
# MAGIC    interpretation guidance, or limitations of the data.
# MAGIC
# MAGIC Decide which tool to call based on the question. For mixed questions, call
# MAGIC `query_network_data` first to get numbers, then `lookup_bdc_methodology` to
# MAGIC explain what they mean. Synthesize a final answer that cites which tool each
# MAGIC piece came from."""
# MAGIC
# MAGIC
# MAGIC def _resolve_end_user(
# MAGIC     custom_inputs: Optional[dict], context: Optional[ChatContext]
# MAGIC ) -> str:
# MAGIC     """Best-effort resolution of the calling user's email."""
# MAGIC     if custom_inputs and isinstance(custom_inputs, dict):
# MAGIC         email = custom_inputs.get("end_user_email")
# MAGIC         if email:
# MAGIC             return email
# MAGIC     # Mosaic AI Agent Framework injects request context as env vars in serving
# MAGIC     for var in ("DATABRICKS_REQUEST_USER", "DB_REQUEST_USER_NAME"):
# MAGIC         val = os.environ.get(var)
# MAGIC         if val:
# MAGIC             return val
# MAGIC     # Fallback: in interactive contexts, the workspace identity is the caller
# MAGIC     try:
# MAGIC         return WorkspaceClient().current_user.me().user_name or "unknown"
# MAGIC     except Exception:
# MAGIC         return "unknown"
# MAGIC
# MAGIC
# MAGIC def _to_lc_messages(messages: list[ChatAgentMessage]) -> list[BaseMessage]:
# MAGIC     out: list[BaseMessage] = []
# MAGIC     for m in messages:
# MAGIC         if m.role == "user":
# MAGIC             out.append(HumanMessage(content=m.content or ""))
# MAGIC         elif m.role == "assistant":
# MAGIC             out.append(AIMessage(content=m.content or ""))
# MAGIC         elif m.role == "system":
# MAGIC             out.append(SystemMessage(content=m.content or ""))
# MAGIC     return out
# MAGIC
# MAGIC
# MAGIC class _AgentState(TypedDict):
# MAGIC     messages: Annotated[list[BaseMessage], add_messages]
# MAGIC     end_user_email: str
# MAGIC
# MAGIC
# MAGIC class NetworkAnalyticsSupervisor(ChatAgent):
# MAGIC     def __init__(self):
# MAGIC         self._llm = ChatDatabricks(endpoint=ROUTER_LLM, temperature=0)
# MAGIC         self._genie = GenieAgent(
# MAGIC             genie_space_id=GENIE_SPACE_ID,
# MAGIC             genie_agent_name="network_analytics_genie",
# MAGIC             description="Queries network analytics Delta tables in cmegdemos_catalog.network_analytics_enablement.",
# MAGIC         )
# MAGIC         self._ka_client = mlflow.deployments.get_deploy_client("databricks")
# MAGIC         self._graph = self._build_graph()
# MAGIC
# MAGIC     # ---------- tools ----------
# MAGIC     def _genie_tool(self):
# MAGIC         genie = self._genie
# MAGIC
# MAGIC         @tool
# MAGIC         def query_network_data(question: str) -> str:
# MAGIC             """Run a question against the Network Analytics Genie space.
# MAGIC             Use for counts, aggregations, row-level data from network tables."""
# MAGIC             with mlflow.start_span(
# MAGIC                 name="genie_call", span_type=SpanType.TOOL
# MAGIC             ) as span:
# MAGIC                 span.set_inputs({"question": question})
# MAGIC                 result = genie.invoke({"messages": [HumanMessage(content=question)]})
# MAGIC                 messages = result.get("messages", [])
# MAGIC                 answer = messages[-1].content if messages else ""
# MAGIC                 span.set_outputs({"answer": answer})
# MAGIC                 return answer
# MAGIC
# MAGIC         return query_network_data
# MAGIC
# MAGIC     def _ka_tool(self):
# MAGIC         client = self._ka_client
# MAGIC
# MAGIC         @tool
# MAGIC         def lookup_bdc_methodology(question: str) -> str:
# MAGIC             """Ask the FCC BDC Methodology knowledge assistant.
# MAGIC             Use for definitions, methodology, technology codes, limitations."""
# MAGIC             with mlflow.start_span(
# MAGIC                 name="ka_call", span_type=SpanType.TOOL
# MAGIC             ) as span:
# MAGIC                 span.set_inputs({"question": question})
# MAGIC                 resp = client.predict(
# MAGIC                     endpoint=KA_ENDPOINT,
# MAGIC                     inputs={
# MAGIC                         "input": [{"role": "user", "content": question}],
# MAGIC                         "stream": False,
# MAGIC                     },
# MAGIC                 )
# MAGIC                 # Mosaic AI Agent endpoints return Responses-API-shaped output
# MAGIC                 text_parts: list[str] = []
# MAGIC                 if isinstance(resp, dict):
# MAGIC                     for item in resp.get("output", []) or []:
# MAGIC                         if item.get("type") == "message":
# MAGIC                             for c in item.get("content", []) or []:
# MAGIC                                 if c.get("type") == "output_text":
# MAGIC                                     text_parts.append(c.get("text", ""))
# MAGIC                 answer = "\n".join(text_parts).strip() or json.dumps(resp)[:2000]
# MAGIC                 span.set_outputs({"answer": answer})
# MAGIC                 return answer
# MAGIC
# MAGIC         return lookup_bdc_methodology
# MAGIC
# MAGIC     # ---------- graph ----------
# MAGIC     def _build_graph(self):
# MAGIC         tools = [self._genie_tool(), self._ka_tool()]
# MAGIC         tool_node = ToolNode(tools)
# MAGIC         llm_with_tools = self._llm.bind_tools(tools)
# MAGIC
# MAGIC         def call_model(state: _AgentState):
# MAGIC             msgs = state["messages"]
# MAGIC             if not any(isinstance(m, SystemMessage) for m in msgs):
# MAGIC                 msgs = [SystemMessage(content=SUPERVISOR_SYSTEM)] + msgs
# MAGIC             ai = llm_with_tools.invoke(msgs)
# MAGIC             return {"messages": [ai]}
# MAGIC
# MAGIC         def should_continue(state: _AgentState):
# MAGIC             last = state["messages"][-1]
# MAGIC             if isinstance(last, AIMessage) and last.tool_calls:
# MAGIC                 return "tools"
# MAGIC             return END
# MAGIC
# MAGIC         g = StateGraph(_AgentState)
# MAGIC         g.add_node("supervisor", call_model)
# MAGIC         g.add_node("tools", tool_node)
# MAGIC         g.set_entry_point("supervisor")
# MAGIC         g.add_conditional_edges("supervisor", should_continue, {"tools": "tools", END: END})
# MAGIC         g.add_edge("tools", "supervisor")
# MAGIC         return g.compile()
# MAGIC
# MAGIC     # ---------- ChatAgent interface ----------
# MAGIC     @mlflow.trace(span_type=SpanType.AGENT, name="supervisor")
# MAGIC     def predict(
# MAGIC         self,
# MAGIC         messages: list[ChatAgentMessage],
# MAGIC         context: Optional[ChatContext] = None,
# MAGIC         custom_inputs: Optional[dict[str, Any]] = None,
# MAGIC     ) -> ChatAgentResponse:
# MAGIC         end_user = _resolve_end_user(custom_inputs, context)
# MAGIC         try:
# MAGIC             mlflow.update_current_trace(
# MAGIC                 tags={
# MAGIC                     "end_user_email": end_user,
# MAGIC                     "supervisor.router_llm": ROUTER_LLM,
# MAGIC                     "supervisor.genie_space": GENIE_SPACE_ID,
# MAGIC                     "supervisor.ka_endpoint": KA_ENDPOINT,
# MAGIC                 }
# MAGIC             )
# MAGIC         except Exception:
# MAGIC             pass
# MAGIC
# MAGIC         lc_msgs = _to_lc_messages(messages)
# MAGIC         result = self._graph.invoke(
# MAGIC             {"messages": lc_msgs, "end_user_email": end_user}
# MAGIC         )
# MAGIC         out_messages: list[ChatAgentMessage] = []
# MAGIC         for m in result["messages"]:
# MAGIC             if isinstance(m, AIMessage) and m.content:
# MAGIC                 out_messages.append(
# MAGIC                     ChatAgentMessage(
# MAGIC                         id=str(uuid.uuid4()),
# MAGIC                         role="assistant",
# MAGIC                         content=m.content,
# MAGIC                     )
# MAGIC                 )
# MAGIC             elif isinstance(m, ToolMessage):
# MAGIC                 out_messages.append(
# MAGIC                     ChatAgentMessage(
# MAGIC                         id=str(uuid.uuid4()),
# MAGIC                         role="tool",
# MAGIC                         content=str(m.content),
# MAGIC                         name=getattr(m, "name", None),
# MAGIC                         tool_call_id=getattr(m, "tool_call_id", None),
# MAGIC                     )
# MAGIC                 )
# MAGIC         if not out_messages:
# MAGIC             out_messages = [
# MAGIC                 ChatAgentMessage(
# MAGIC                     id=str(uuid.uuid4()),
# MAGIC                     role="assistant",
# MAGIC                     content="(no response)",
# MAGIC                 )
# MAGIC             ]
# MAGIC         return ChatAgentResponse(
# MAGIC             messages=out_messages,
# MAGIC             custom_outputs={"end_user_email": end_user},
# MAGIC         )
# MAGIC
# MAGIC     def predict_stream(
# MAGIC         self,
# MAGIC         messages: list[ChatAgentMessage],
# MAGIC         context: Optional[ChatContext] = None,
# MAGIC         custom_inputs: Optional[dict[str, Any]] = None,
# MAGIC     ) -> Generator[ChatAgentChunk, None, None]:
# MAGIC         resp = self.predict(messages, context, custom_inputs)
# MAGIC         for m in resp.messages:
# MAGIC             yield ChatAgentChunk(delta=m)
# MAGIC
# MAGIC
# MAGIC mlflow.models.set_model(NetworkAnalyticsSupervisor())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Local smoke test before logging

# COMMAND ----------

import importlib, supervisor_agent
importlib.reload(supervisor_agent)
from supervisor_agent import NetworkAnalyticsSupervisor
from mlflow.types.agent import ChatAgentMessage

agent = NetworkAnalyticsSupervisor()
resp = agent.predict(
    messages=[
        ChatAgentMessage(role="user", content="What does mindown represent in the FCC BDC data?", id="local-1")
    ],
    custom_inputs={"end_user_email": "razi.bayati@databricks.com"},
)
print(resp.messages[-1].content[:500])
print("custom_outputs:", resp.custom_outputs)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Log + register

# COMMAND ----------

import mlflow
from importlib.metadata import version as _v

mlflow.set_registry_uri("databricks-uc")
exp = mlflow.set_experiment(f"/Users/{spark.sql('SELECT current_user()').first()[0]}/network_analytics_supervisor")
print("experiment_id:", exp.experiment_id)

with mlflow.start_run(run_name="supervisor-v1") as run:
    logged = mlflow.pyfunc.log_model(
        artifact_path="agent",
        python_model="supervisor_agent.py",
        pip_requirements=[
            f"mlflow=={_v('mlflow')}",
            f"databricks-agents=={_v('databricks-agents')}",
            f"databricks-langchain=={_v('databricks-langchain')}",
            f"langgraph=={_v('langgraph')}",
            "langgraph-supervisor",
            "langchain-core",
        ],
        input_example={
            "messages": [{"role": "user", "content": "What is the average mindown in downtown Seattle?"}],
            "custom_inputs": {"end_user_email": "razi.bayati@databricks.com"},
        },
        resources=[
            mlflow.models.resources.DatabricksGenieSpace(genie_space_id=GENIE_SPACE_ID),
            mlflow.models.resources.DatabricksServingEndpoint(endpoint_name=KA_ENDPOINT),
            mlflow.models.resources.DatabricksServingEndpoint(endpoint_name=ROUTER_LLM),
        ],
    )
    print("logged:", logged.model_uri)

registered = mlflow.register_model(model_uri=logged.model_uri, name=UC_MODEL_NAME)
print("registered:", UC_MODEL_NAME, "version", registered.version)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Deploy

# COMMAND ----------

from databricks import agents

deployment = agents.deploy(
    model_name=UC_MODEL_NAME,
    model_version=registered.version,
    endpoint_name=ENDPOINT_NAME,
    scale_to_zero=True,
    tags={"project": "network_analytics_enablement", "owner": "razi.bayati"},
)
print("endpoint:", deployment.endpoint_name)
print("review_app_url:", getattr(deployment, "review_app_url", None))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Smoke test the deployed endpoint

# COMMAND ----------

import time, mlflow
client = mlflow.deployments.get_deploy_client("databricks")

# Wait for endpoint readiness
from databricks.sdk import WorkspaceClient
w = WorkspaceClient()
for _ in range(60):
    ep = w.serving_endpoints.get(ENDPOINT_NAME)
    if ep.state and ep.state.ready and str(ep.state.ready) == "READY":
        break
    time.sleep(15)

for q in [
    "How many H3 hexagons in downtown Seattle have mindown >= 100?",
    "What does low_latency = true actually guarantee?",
    "Are there buildings with weak coverage near a tower? Why might that be?",
]:
    print("Q:", q)
    out = client.predict(
        endpoint=ENDPOINT_NAME,
        inputs={
            "messages": [{"role": "user", "content": q}],
            "custom_inputs": {"end_user_email": "razi.bayati@databricks.com"},
        },
    )
    last = out.get("messages", [])[-1] if out.get("messages") else {}
    print("A:", (last.get("content") or "")[:400])
    print("custom_outputs:", out.get("custom_outputs"))
    print("---")
