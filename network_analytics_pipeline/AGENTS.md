# Declarative Automation Bundles Project

This project uses Declarative Automation Bundles (formerly Databricks Asset Bundles) for deployment.

## Prerequisites

Install the Databricks CLI (>= v0.288.0) if not already installed:

- macOS: `brew tap databricks/tap && brew install databricks`
- Linux: `curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh | sh`
- Windows: `winget install Databricks.DatabricksCLI`

Verify: `databricks -v`

## For AI Agents

Read the `databricks-core` skill for CLI basics, authentication, and deployment workflow.
Read the `databricks-pipelines` skill for pipeline-specific guidance.

If skills are not available, install them: `databricks experimental aitools install`

## Project layout

- `databricks.yml` — bundle and variable definitions
- `resources/network_analytics.pipeline.yml` — Lakeflow pipeline resource
- `src/bronze/*.py` — raw extracts, structural Expectations
- `src/silver/*.py` — typed/filtered Seattle tables, business-rule Expectations
- `src/gold/*.py` — joined business analysis, hard-contract Expectations
