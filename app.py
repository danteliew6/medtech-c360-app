"""
MedTech Customer 360 — sample Databricks App (FastAPI).
Reads the medtech_c360 schema via the SQL warehouse (Statement Execution API).
Dual-mode auth: injected service principal in Databricks Apps, CLI profile locally.
"""
import os
import re
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementParameterListItem

CATALOG = os.environ.get("CATALOG", "dante_classic_stable_catalog")
SCHEMA = os.environ.get("SCHEMA", "medtech_c360")
WAREHOUSE_ID = os.environ.get("WAREHOUSE_ID", "114b2f7bfa1273b1")
IS_APP = bool(os.environ.get("DATABRICKS_APP_NAME"))

_w = None
def w():
    global _w
    if _w is None:
        _w = WorkspaceClient() if IS_APP else WorkspaceClient(profile=os.environ.get("DATABRICKS_PROFILE", "fevm-dante-classic-stable"))
    return _w


def q(sql, params=None):
    p = [StatementParameterListItem(name=k, value=str(v)) for k, v in (params or {}).items()] or None
    r = w().statement_execution.execute_statement(
        warehouse_id=WAREHOUSE_ID, statement=sql, catalog=CATALOG, schema=SCHEMA,
        parameters=p, wait_timeout="30s")
    import time
    while r.status.state.value in ("PENDING", "RUNNING"):
        time.sleep(1); r = w().statement_execution.get_statement(r.statement_id)
    if r.status.state.value != "SUCCEEDED":
        raise HTTPException(500, r.status.error.message if r.status.error else "query failed")
    cols = [c.name for c in r.manifest.schema.columns]
    rows = r.result.data_array or [] if r.result else []
    out = []
    for row in rows:
        d = {}
        for c, v in zip(cols, row):
            d[c] = v
        out.append(d)
    return out


app = FastAPI(title="MedTech Customer 360")


@app.get("/api/health")
def health():
    return {"ok": True, "catalog": CATALOG, "schema": SCHEMA, "mode": "app" if IS_APP else "local"}


@app.get("/api/overview")
def overview():
    kpi = q("""SELECT
      ROUND((SELECT SUM(net_amount_usd) FROM fact_orders WHERE year(order_date)=year(current_date()))/1e6,1) AS revenue_ytd_m,
      (SELECT COUNT(*) FROM dim_account) AS accounts,
      ROUND((SELECT SUM(amount_usd) FROM fact_opportunity WHERE is_closed=false)/1e6,1) AS pipeline_m,
      ROUND((SELECT 100.0*COUNT_IF(is_won)/NULLIF(COUNT_IF(is_closed),0) FROM fact_opportunity WHERE year(actual_close_date)=year(current_date())),1) AS win_rate_pct,
      (SELECT COUNT(*) FROM vw_account_360 WHERE churn_risk='At Risk') AS at_risk,
      (SELECT COUNT(*) FROM fact_complaint WHERE is_mdr=true AND resolution_status IN ('Open','Investigating')) AS open_mdr""")[0]
    by_bu = q("""SELECT business_unit, ROUND(SUM(net_revenue_usd),0) AS revenue
                 FROM vw_revenue_monthly WHERE year=year(current_date())
                 GROUP BY business_unit ORDER BY revenue DESC""")
    return {"kpi": kpi, "by_bu": by_bu}


@app.get("/api/accounts")
def accounts(query: str = "", limit: int = 30):
    lim = max(1, min(int(limit), 100))
    cols = """account_id, account_name, account_type, region, account_tier, churn_risk,
              ROUND(revenue_ytd_usd,0) AS revenue_ytd, ROUND(open_pipeline_usd,0) AS open_pipeline,
              days_since_last_order"""
    if query:
        return q(f"""SELECT {cols} FROM vw_account_360 WHERE lower(account_name) LIKE lower(:q)
                     ORDER BY revenue_ytd_usd DESC LIMIT {lim}""", {"q": f"%{query}%"})
    return q(f"SELECT {cols} FROM vw_account_360 ORDER BY revenue_ytd_usd DESC LIMIT {lim}")


@app.get("/api/accounts/{account_id}")
def account_detail(account_id: str):
    if not re.match(r"^ACC-\d+$", account_id):
        raise HTTPException(400, "bad account id")
    hdr = q("SELECT * FROM vw_account_360 WHERE account_id = :id", {"id": account_id})
    if not hdr:
        raise HTTPException(404, "account not found")
    by_bu = q("""SELECT p.business_unit, ROUND(SUM(o.net_amount_usd),0) AS revenue, COUNT(*) AS lines
                 FROM fact_orders o JOIN dim_product p USING (product_id)
                 WHERE o.account_id = :id GROUP BY p.business_unit ORDER BY revenue DESC""", {"id": account_id})
    orders = q("""SELECT o.order_date, p.product_name, p.business_unit, o.quantity,
                    ROUND(o.net_amount_usd,0) AS net_amount, o.channel
                  FROM fact_orders o JOIN dim_product p USING (product_id)
                  WHERE o.account_id = :id ORDER BY o.order_date DESC LIMIT 15""", {"id": account_id})
    opps = q("""SELECT stage, business_unit, ROUND(amount_usd,0) AS amount, probability_pct, expected_close_date
                FROM fact_opportunity WHERE account_id = :id AND is_closed=false
                ORDER BY amount_usd DESC LIMIT 15""", {"id": account_id})
    complaints = q("""SELECT c.date_reported, p.product_name, c.complaint_category, c.severity,
                        c.is_mdr, c.resolution_status
                      FROM fact_complaint c JOIN dim_product p USING (product_id)
                      WHERE c.account_id = :id ORDER BY c.date_reported DESC LIMIT 15""", {"id": account_id})
    return {"header": hdr[0], "by_bu": by_bu, "orders": orders, "opportunities": opps, "complaints": complaints}


# ---- static frontend ----
_static = os.path.join(os.path.dirname(__file__), "static")
app.mount("/static", StaticFiles(directory=_static), name="static")


@app.get("/")
def index():
    return FileResponse(os.path.join(_static, "index.html"))
