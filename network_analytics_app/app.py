"""RF Engineer H3 viz console — Dash + dash-leaflet.

Style adapted from databricks-industry-solutions/geospatial-h3-viz-app:
  - Full-viewport dark Leaflet map
  - H3 hex polygons via Databricks SQL `h3_boundaryasgeojson`
  - Adaptive resolution via `h3_toparent` based on zoom level
  - Septile log color scale + legend in upper-right
  - Top dark control strip with metric / aggregation / refresh

RF-specific extensions:
  - Metric switcher (prb_utilization_pct, latency_ms, packet_loss_pct,
    throughput_mbps, demand_users, best_download_mbps, building_count)
  - Click a hex → side drawer with buildings, KPIs, tower KPI sparkline
"""

from __future__ import annotations

import json
import os
from datetime import datetime
from typing import Optional

import dash
import dash_bootstrap_components as dbc
import dash_leaflet as dl
import flask
import numpy as np
import pandas as pd
import plotly.graph_objects as go
from dash import Input, Output, State, dcc, html, no_update
from dash_extensions.javascript import assign
from databricks import sql
from databricks.sdk.core import Config
from shapely.geometry import Polygon

# ---------------- ENV ----------------

DATABRICKS_WAREHOUSE_ID = os.getenv("DATABRICKS_WAREHOUSE_ID", "9cd919d96b11bf1c")
DEFAULT_CATALOG = os.getenv("DEFAULT_CATALOG", "cmegdemos_catalog")
DEFAULT_SCHEMA = os.getenv("DEFAULT_SCHEMA", "network_analytics_enablement")
DEFAULT_TABLE = os.getenv("DEFAULT_TABLE", "ops_app_gold_downtown_building_coverage")
DEFAULT_METRIC = os.getenv("DEFAULT_METRIC", "prb_utilization_pct")
H3_COLUMN = "h3_res9_id"
H3_NATIVE_RES = 9

METRIC_CHOICES = [
    {"label": "PRB utilization (%)", "value": "prb_utilization_pct", "agg": "AVG", "fmt": "{:.1f}%"},
    {"label": "Latency (ms)",        "value": "latency_ms",          "agg": "AVG", "fmt": "{:.0f} ms"},
    {"label": "Packet loss (%)",     "value": "packet_loss_pct",     "agg": "AVG", "fmt": "{:.2f}%"},
    {"label": "Throughput (Mbps)",   "value": "throughput_mbps",     "agg": "AVG", "fmt": "{:.0f} Mbps"},
    {"label": "Demand (users)",      "value": "demand_users",        "agg": "SUM", "fmt": "{:.0f}"},
    {"label": "Best 5G download",    "value": "best_download_mbps",  "agg": "MAX", "fmt": "{:.0f} Mbps"},
    {"label": "Building count",      "value": "__count",             "agg": "COUNT", "fmt": "{:.0f}"},
]

METRIC_BY_VALUE = {m["value"]: m for m in METRIC_CHOICES}


# ---------------- AUTH / SQL ----------------

_CFG = None


def _cfg() -> Config:
    global _CFG
    if _CFG is None:
        _CFG = Config()
    return _CFG


def get_databricks_host() -> str:
    host = os.getenv("DATABRICKS_HOST") or _cfg().host
    return host.replace("https://", "").replace("http://", "").rstrip("/")


def get_user_obo_token() -> Optional[str]:
    """Return user's OBO token from request headers if present."""
    try:
        return flask.request.headers.get("X-Forwarded-Access-Token")
    except RuntimeError:
        return None


def sql_query(stmt: str) -> pd.DataFrame:
    """Execute a SQL query as the app's service principal.

    The SP already holds USE CATALOG / USE SCHEMA / SELECT grants on the
    relevant tables and CAN_USE on the warehouse. We deliberately skip OBO
    because the default app user-authorization scopes do not include `sql`,
    which causes 403 from the SQL warehouse.
    """
    with sql.connect(
        http_path=f"/sql/1.0/warehouses/{DATABRICKS_WAREHOUSE_ID}",
        server_hostname=get_databricks_host(),
        credentials_provider=lambda: _cfg().authenticate,
    ) as conn:
        with conn.cursor() as cur:
            cur.execute(stmt)
            cols = [d[0] for d in cur.description]
            rows = cur.fetchall()
    return pd.DataFrame(rows, columns=cols)


# ---------------- H3 HELPERS ----------------

def zoom_to_h3_res(zoom: float, native: int = H3_NATIVE_RES) -> int:
    """Map Leaflet zoom level to H3 resolution.

    Native source is res 9, so we can only roll *up* (coarser). At wide zoom
    we want to coarsen so each hex covers more area; at tight zoom we keep
    the native res.
    """
    if zoom is None:
        return native
    if zoom < 12:
        r = 7
    elif zoom < 14:
        r = 8
    else:
        r = 9
    return min(r, native)


def bounds_to_wkt(bounds) -> Optional[str]:
    if not bounds:
        return None
    sw, ne = bounds  # [[lat,lon],[lat,lon]]
    coords = [
        (sw[1], sw[0]),
        (sw[1], ne[0]),
        (ne[1], ne[0]),
        (ne[1], sw[0]),
        (sw[1], sw[0]),
    ]
    return Polygon(coords).wkt


def fetch_hex_aggregate(catalog, schema, table, metric, resolution, bounds) -> pd.DataFrame:
    spec = METRIC_BY_VALUE[metric]
    if metric == "__count":
        value_expr = "COUNT(*) AS value"
    else:
        value_expr = f"{spec['agg']}({metric}) AS value"

    bounds_wkt = bounds_to_wkt(bounds)
    where = "1=1"
    if bounds_wkt:
        where = (
            f"h3_toparent({H3_COLUMN}, {resolution}) IN "
            f"(SELECT EXPLODE(H3_COVERASH3('{bounds_wkt}', {resolution})))"
        )
    stmt = f"""
        WITH cell_agg AS (
          SELECT
            h3_toparent({H3_COLUMN}, {resolution}) AS h3_cell_id,
            COUNT(*) AS building_count,
            {value_expr}
          FROM {catalog}.{schema}.{table}
          WHERE {where}
            AND {H3_COLUMN} IS NOT NULL
          GROUP BY 1
        )
        SELECT h3_cell_id AS hex_id,
               h3_boundaryasgeojson(h3_cell_id) AS hex_boundary,
               building_count,
               value
        FROM cell_agg
        WHERE value IS NOT NULL
    """
    return sql_query(stmt)


def fetch_buildings_in_hex(catalog, schema, table, hex_id, resolution) -> pd.DataFrame:
    stmt = f"""
        SELECT
          building_id,
          centroid_lon, centroid_lat,
          best_download_mbps,
          nearest_tower_id, distance_to_tower_m,
          demand_users, traffic_mix, indoor_penetration_factor,
          prb_utilization_pct, latency_ms, packet_loss_pct, throughput_mbps,
          building_service_risk_band
        FROM {catalog}.{schema}.{table}
        WHERE h3_toparent({H3_COLUMN}, {resolution}) = '{hex_id}'
        ORDER BY prb_utilization_pct DESC NULLS LAST, demand_users DESC NULLS LAST
        LIMIT 200
    """
    return sql_query(stmt)


def fetch_tower_history(tower_id: int) -> pd.DataFrame:
    stmt = f"""
        SELECT date_trunc('HOUR', event_ts) AS event_ts,
               AVG(prb_utilization_pct) AS prb,
               AVG(latency_ms)          AS latency,
               AVG(packet_loss_pct)     AS loss,
               AVG(throughput_mbps)     AS throughput
        FROM {DEFAULT_CATALOG}.{DEFAULT_SCHEMA}.ops_app_bronze_tower_hourly_kpis
        WHERE tower_id = {int(tower_id)}
        GROUP BY date_trunc('HOUR', event_ts)
        ORDER BY event_ts
    """
    df = sql_query(stmt)
    if not df.empty:
        df["event_ts"] = pd.to_datetime(df["event_ts"], utc=True, errors="coerce")
    return df


# ---------------- COLOR / LEGEND ----------------

PALETTE = ["#FED976", "#FEB24C", "#FD8D3C", "#FC4E2A", "#E31A1C", "#BD0026", "#800026"]


def log_breaks(values: np.ndarray, n: int = 7) -> list[float]:
    pos = values[values > 0]
    if len(pos) == 0:
        return list(np.linspace(0, 1, n + 1))
    lo, hi = float(np.log10(pos.min())), float(np.log10(pos.max()))
    if hi <= lo:
        hi = lo + 1
    return [10 ** v for v in np.linspace(lo, hi, n + 1)]


def color_for(value: float, breaks: list[float]) -> str:
    for i, ub in enumerate(breaks[1:]):
        if value <= ub:
            return PALETTE[min(i, len(PALETTE) - 1)]
    return PALETTE[-1]


def build_geojson(df: pd.DataFrame, breaks: list[float], metric: str) -> dict:
    spec = METRIC_BY_VALUE[metric]
    fmt = spec["fmt"]
    features = []
    for _, row in df.iterrows():
        boundary = json.loads(row["hex_boundary"]) if isinstance(row["hex_boundary"], str) else row["hex_boundary"]
        v = float(row["value"])
        try:
            value_label = fmt.format(v)
        except Exception:
            value_label = f"{v:.2f}"
        features.append({
            "type": "Feature",
            "id": row["hex_id"],
            "geometry": boundary,
            "properties": {
                "hex_id": row["hex_id"],
                "value": v,
                "value_label": value_label,
                "building_count": int(row["building_count"]),
                "fillColor": color_for(v, breaks),
            },
        })
    return {"type": "FeatureCollection", "features": features}


def make_legend(breaks: list[float], metric: str) -> html.Div:
    spec = METRIC_BY_VALUE[metric]
    fmt = spec["fmt"]
    swatches = []
    for i, color in enumerate(PALETTE):
        lo = breaks[i]
        hi = breaks[i + 1]
        try:
            label = f"{fmt.format(lo)} – {fmt.format(hi)}"
        except Exception:
            label = f"{lo:.2f} – {hi:.2f}"
        swatches.append(
            html.Div([
                html.Div(style={
                    "backgroundColor": color, "width": "18px", "height": "18px",
                    "display": "inline-block", "border": "1px solid #000",
                    "marginRight": "8px", "verticalAlign": "middle",
                }),
                html.Span(label, style={
                    "fontSize": "12px", "color": "#FFFFFF",
                    "fontFamily": "Helvetica",
                }),
            ], style={"marginBottom": "4px"})
        )
    return html.Div(
        [
            html.Div(spec["label"], style={
                "marginBottom": "8px", "fontSize": "14px", "color": "#FFFFFF",
                "fontFamily": "Helvetica", "fontWeight": "bold",
            }),
            html.Div(swatches),
        ],
        style={
            "position": "absolute", "top": "92px", "right": "16px",
            "backgroundColor": "#3A3A3A", "padding": "10px 14px",
            "borderRadius": "6px", "boxShadow": "0 0 10px rgba(0,0,0,0.4)",
            "zIndex": "1000", "fontFamily": "Helvetica",
        },
    )


# ---------------- DASH APP ----------------

app = dash.Dash(
    __name__,
    external_stylesheets=[dbc.themes.DARKLY, dbc.icons.BOOTSTRAP],
    title="RF Engineer · H3 viz",
    suppress_callback_exceptions=True,
)
server = app.server  # for Databricks Apps gunicorn-less deploy via flask

# JS style function: read fillColor from feature.properties
hex_style = assign("""function(feature) {
    return {
        fillColor: feature.properties.fillColor,
        color: feature.properties.fillColor,
        weight: 1,
        opacity: 0.9,
        fillOpacity: 0.65,
    };
}""")

hex_hover_style = assign("""function(feature) {
    return {
        weight: 3,
        color: '#ffffff',
        dashArray: '',
        fillOpacity: 0.85,
    };
}""")

# Initial map view: downtown Seattle
SEATTLE_CENTER = [47.6097, -122.3331]


def control_strip() -> dbc.Container:
    return dbc.Container(
        dbc.Row(
            [
                dbc.Col([
                    html.Label("Hero metric", style={"color": "#FFFFFF", "fontSize": "12px"}),
                    dcc.Dropdown(
                        id="metric-dropdown",
                        options=[{"label": m["label"], "value": m["value"]} for m in METRIC_CHOICES],
                        value=DEFAULT_METRIC,
                        clearable=False,
                        style={"width": "240px", "color": "#000"},
                    ),
                ], width="auto"),
                dbc.Col([
                    html.Label("Min building count", style={"color": "#FFFFFF", "fontSize": "12px"}),
                    dcc.Slider(
                        id="min-count", min=1, max=20, step=1, value=1,
                        marks={1: "1", 5: "5", 10: "10", 20: "20"},
                    ),
                ], width=2),
                dbc.Col([
                    html.Label("Resolution", style={"color": "#FFFFFF", "fontSize": "12px"}),
                    dcc.RadioItems(
                        id="resolution-mode",
                        options=[
                            {"label": "Auto (zoom)", "value": "auto"},
                            {"label": "9 (block)", "value": 9},
                            {"label": "8", "value": 8},
                            {"label": "7", "value": 7},
                        ],
                        value="auto",
                        inline=True,
                        labelStyle={"color": "#FFFFFF", "marginRight": "10px", "fontSize": "13px"},
                    ),
                ], width="auto"),
                dbc.Col([
                    html.Label(" ", style={"display": "block", "color": "#FFFFFF", "fontSize": "12px"}),
                    dbc.Button("Refresh", id="refresh-btn", color="success", size="sm",
                               style={"width": "120px"}),
                ], width="auto"),
                dbc.Col([
                    html.Div(id="status-line", style={
                        "color": "#cfd2d6", "fontSize": "12px",
                        "fontFamily": "Helvetica", "paddingTop": "26px",
                    }),
                ]),
            ],
            align="end",
        ),
        fluid=True,
        style={
            "backgroundColor": "#2a2a2a",
            "padding": "12px 16px",
            "borderBottom": "1px solid #444",
        },
    )


def side_drawer() -> html.Div:
    return html.Div(
        id="drawer",
        children=[
            html.Div("Click a hex on the map to drill in.", style={
                "color": "#9aa0a6", "padding": "16px", "fontStyle": "italic",
                "fontFamily": "Helvetica", "fontSize": "13px",
            }),
        ],
        style={
            "position": "absolute",
            "top": "92px", "left": "16px", "bottom": "16px",
            "width": "360px",
            "backgroundColor": "rgba(28,30,34,0.95)",
            "borderRadius": "8px",
            "boxShadow": "0 4px 20px rgba(0,0,0,0.5)",
            "zIndex": "1000",
            "overflowY": "auto",
            "padding": "10px",
            "backdropFilter": "blur(8px)",
            "border": "1px solid #444",
        },
    )


app.layout = html.Div(
    [
        # title strip
        html.Div(
            [
                html.Span("RF Engineer Console · ", style={
                    "color": "#3ec487", "fontWeight": "600",
                    "fontSize": "16px", "fontFamily": "Helvetica",
                }),
                html.Span("Downtown Seattle H3 grid", style={
                    "color": "#FFFFFF", "fontSize": "16px",
                    "fontFamily": "Helvetica",
                }),
                html.Span(
                    f"  ·  {DEFAULT_CATALOG}.{DEFAULT_SCHEMA}.{DEFAULT_TABLE}",
                    style={"color": "#9aa0a6", "fontSize": "12px",
                           "fontFamily": "Helvetica", "marginLeft": "8px"},
                ),
            ],
            style={
                "backgroundColor": "#1c1e22", "padding": "10px 16px",
                "borderBottom": "1px solid #333",
            },
        ),
        control_strip(),
        # main map area
        html.Div(
            [
                dl.Map(
                    id="leaflet-map",
                    center=SEATTLE_CENTER, zoom=14,
                    style={"width": "100%", "height": "calc(100vh - 92px)"},
                    children=[
                        dl.TileLayer(
                            url="https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png",
                            attribution='© OpenStreetMap, © CARTO',
                        ),
                        dl.GeoJSON(
                            id="h3-grid",
                            data={"type": "FeatureCollection", "features": []},
                            options=dict(style=hex_style),
                            hoverStyle=hex_hover_style,
                        ),
                    ],
                ),
                html.Div(id="legend-container"),
                side_drawer(),
            ],
            style={"position": "relative"},
        ),
        # state stores
        dcc.Store(id="selected-hex"),
        dcc.Store(id="last-zoom", data=14),
    ],
    style={
        "backgroundColor": "#0f1115",
        "minHeight": "100vh",
        "fontFamily": "Helvetica, Arial, sans-serif",
    },
)


# ---------------- CALLBACKS ----------------

@app.callback(
    Output("h3-grid", "data"),
    Output("legend-container", "children"),
    Output("status-line", "children"),
    Input("refresh-btn", "n_clicks"),
    Input("metric-dropdown", "value"),
    Input("min-count", "value"),
    Input("resolution-mode", "value"),
    Input("leaflet-map", "zoom"),
    Input("leaflet-map", "bounds"),
    prevent_initial_call=False,
)
def update_grid(n_clicks, metric, min_count, res_mode, zoom, bounds):
    t0 = datetime.utcnow()
    try:
        if res_mode == "auto":
            resolution = zoom_to_h3_res(zoom or 14)
        else:
            resolution = int(res_mode)

        df = fetch_hex_aggregate(
            DEFAULT_CATALOG, DEFAULT_SCHEMA, DEFAULT_TABLE, metric, resolution, bounds
        )
        df = df[df["building_count"] >= int(min_count)]
        if df.empty:
            return ({"type": "FeatureCollection", "features": []}, no_update,
                    f"No hexes at res {resolution}. Try a wider area or lower min-count.")

        breaks = log_breaks(df["value"].astype(float).to_numpy())
        gj = build_geojson(df, breaks, metric)
        legend = make_legend(breaks, metric)
        dt_ms = (datetime.utcnow() - t0).total_seconds() * 1000
        spec = METRIC_BY_VALUE[metric]
        status = (
            f"{len(df):,} hexes · res {resolution} · "
            f"metric {spec['label']} · {dt_ms:.0f} ms"
        )
        return gj, legend, status
    except Exception as e:
        return ({"type": "FeatureCollection", "features": []}, no_update,
                f"Error: {e}")


@app.callback(
    Output("drawer", "children"),
    Input("h3-grid", "clickData"),
    State("metric-dropdown", "value"),
    State("resolution-mode", "value"),
    State("leaflet-map", "zoom"),
    prevent_initial_call=True,
)
def on_hex_click(feature, metric, res_mode, zoom):
    if not feature:
        return no_update
    props = feature.get("properties") or feature
    hex_id = props.get("hex_id") or props.get("id")
    if not hex_id:
        return [html.Div("Hex ID missing on click event.",
                         style={"color": "#ec5b62", "padding": "12px"})]
    value = props.get("value", 0)
    value_label = props.get("value_label", str(value))
    building_count = props.get("building_count", 0)
    spec = METRIC_BY_VALUE[metric]

    resolution = zoom_to_h3_res(zoom or 14) if res_mode == "auto" else int(res_mode)

    try:
        buildings = fetch_buildings_in_hex(
            DEFAULT_CATALOG, DEFAULT_SCHEMA, DEFAULT_TABLE, hex_id, resolution
        )
    except Exception as e:
        return [html.Div(f"Failed to fetch buildings: {e}",
                         style={"color": "#ec5b62", "padding": "12px"})]

    children = [
        html.Div([
            html.Div("Selected H3 cell", style={
                "fontSize": "12px", "color": "#9aa0a6", "letterSpacing": "1.5px",
            }),
            html.Div(hex_id, style={
                "fontSize": "13px", "fontFamily": "monospace",
                "color": "#cfd2d6", "marginBottom": "10px",
            }),
            dbc.Row([
                dbc.Col([
                    html.Div(spec["label"], style={"fontSize": "11px", "color": "#9aa0a6"}),
                    html.Div(value_label, style={
                        "fontSize": "20px", "fontWeight": "600", "color": "#FFFFFF",
                    }),
                ], width=6),
                dbc.Col([
                    html.Div("Buildings", style={"fontSize": "11px", "color": "#9aa0a6"}),
                    html.Div(f"{building_count:,}", style={
                        "fontSize": "20px", "fontWeight": "600", "color": "#FFFFFF",
                    }),
                ], width=6),
            ]),
        ], style={"padding": "12px", "borderBottom": "1px solid #333"}),
    ]

    if buildings.empty:
        children.append(html.Div("No buildings in this hex.",
                                 style={"color": "#9aa0a6", "padding": "12px"}))
        return children

    # Tower mix in this hex
    tower_summary = (
        buildings.groupby("nearest_tower_id")
        .agg(
            n=("building_id", "count"),
            avg_prb=("prb_utilization_pct", "mean"),
            avg_lat=("latency_ms", "mean"),
            avg_demand=("demand_users", "mean"),
        )
        .sort_values("n", ascending=False)
        .reset_index()
    )

    tower_rows = []
    for _, t in tower_summary.head(5).iterrows():
        tower_rows.append(
            html.Div([
                html.Div([
                    html.Span(f"Tower {int(t['nearest_tower_id'])}", style={
                        "fontWeight": "600", "color": "#3ec487", "marginRight": "10px",
                    }),
                    html.Span(f"{int(t['n'])} bldgs", style={
                        "color": "#9aa0a6", "fontSize": "11px",
                    }),
                ]),
                html.Div([
                    html.Span(f"PRB {t['avg_prb']:.0f}%", style={"color": "#cfd2d6", "marginRight": "12px"}),
                    html.Span(f"lat {t['avg_lat']:.0f} ms", style={"color": "#cfd2d6", "marginRight": "12px"}),
                    html.Span(f"demand {t['avg_demand']:.0f}", style={"color": "#cfd2d6"}),
                ], style={"fontSize": "12px", "marginTop": "2px"}),
                dbc.Button("View tower history", id={"type": "tower-history", "tower_id": int(t["nearest_tower_id"])},
                           color="link", size="sm",
                           style={"padding": "0", "fontSize": "11px", "marginTop": "4px"}),
            ], style={"padding": "8px 12px", "borderBottom": "1px solid #2a2a2a"})
        )

    children.append(html.Div(
        [html.Div("Towers in this hex", style={
            "padding": "10px 12px 6px", "fontSize": "12px",
            "color": "#9aa0a6", "letterSpacing": "1px",
        })] + tower_rows
    ))

    # Top buildings
    show = buildings.head(10)
    bldg_rows = []
    for _, b in show.iterrows():
        risk_color = {"critical": "#ec5b62", "watch": "#f5b942", "healthy": "#3ec487"}.get(
            str(b.get("building_service_risk_band") or "").lower(), "#9aa0a6"
        )
        bldg_rows.append(
            html.Div([
                html.Div([
                    html.Span(f"#{int(b['building_id'])}", style={
                        "color": "#FFFFFF", "fontWeight": "600",
                    }),
                    html.Span(
                        f" · {b.get('building_service_risk_band', '—')}",
                        style={"color": risk_color, "fontSize": "11px", "marginLeft": "6px"},
                    ),
                ]),
                html.Div([
                    f"PRB {b.get('prb_utilization_pct', 0):.0f}% · "
                    f"lat {b.get('latency_ms', 0):.0f} ms · "
                    f"loss {b.get('packet_loss_pct', 0):.2f}% · "
                    f"demand {b.get('demand_users', 0):.0f}",
                ], style={"fontSize": "11px", "color": "#9aa0a6", "marginTop": "2px"}),
            ], style={"padding": "8px 12px", "borderBottom": "1px solid #2a2a2a"})
        )
    children.append(html.Div(
        [html.Div(f"Top buildings ({len(show)} of {len(buildings)})", style={
            "padding": "10px 12px 6px", "fontSize": "12px",
            "color": "#9aa0a6", "letterSpacing": "1px",
        })] + bldg_rows
    ))

    # Tower history sparkline for the dominant tower
    if not tower_summary.empty:
        dom_tower = int(tower_summary.iloc[0]["nearest_tower_id"])
        try:
            hist = fetch_tower_history(dom_tower)
            if not hist.empty:
                fig = go.Figure()
                fig.add_trace(go.Scatter(
                    x=hist["event_ts"], y=hist["prb"],
                    mode="lines", line=dict(color="#5b8def", width=2),
                    fill="tozeroy", fillcolor="rgba(91,141,239,0.15)",
                    name="PRB %",
                ))
                fig.add_trace(go.Scatter(
                    x=hist["event_ts"], y=hist["latency"],
                    mode="lines", line=dict(color="#f5b942", width=1.5),
                    name="Latency", yaxis="y2",
                ))
                fig.update_layout(
                    title=dict(text=f"Tower {dom_tower} · 14d history",
                               font=dict(color="#FFFFFF", size=12)),
                    template="plotly_dark",
                    height=200,
                    margin=dict(l=10, r=10, t=40, b=20),
                    paper_bgcolor="rgba(0,0,0,0)",
                    plot_bgcolor="rgba(0,0,0,0)",
                    yaxis=dict(title="PRB %", range=[0, 100], color="#5b8def"),
                    yaxis2=dict(title="ms", overlaying="y", side="right", color="#f5b942"),
                    legend=dict(orientation="h", y=-0.2, font=dict(color="#cfd2d6")),
                    showlegend=True,
                )
                children.append(html.Div(
                    dcc.Graph(figure=fig, config={"displayModeBar": False}),
                    style={"padding": "8px"},
                ))
        except Exception:
            pass

    return children


if __name__ == "__main__":
    port = int(os.getenv("DATABRICKS_APP_PORT", "8000"))
    app.run(host="0.0.0.0", port=port, debug=False)
