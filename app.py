"""
MedTech Customer 360 — sample Databricks App (FastAPI).
Reads the medtech_c360 schema via the SQL warehouse (Statement Execution API).
Dual-mode auth: injected service principal in Databricks Apps, CLI profile locally.
"""
import os
import re
import time
from concurrent.futures import ThreadPoolExecutor
from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementParameterListItem

CATALOG = os.environ.get("CATALOG", "dante_classic_stable_catalog")
SCHEMA = os.environ.get("SCHEMA", "medtech_c360")
WAREHOUSE_ID = os.environ.get("WAREHOUSE_ID", "114b2f7bfa1273b1")
IS_APP = bool(os.environ.get("DATABRICKS_APP_NAME"))
ALL_BUS = ["Interventional Systems", "Blood & Cell Technologies", "Cardiovascular",
           "Medical Care Solutions", "Diabetes Care"]

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
    return [dict(zip(cols, row)) for row in rows]


# Run independent queries concurrently — collapses N sequential warehouse
# round-trips (~1s each of fixed overhead) into ~1 round-trip of wall time.
_EXEC = ThreadPoolExecutor(max_workers=8)
def q_many(jobs):
    futs = {k: _EXEC.submit(q, sql, params) for k, (sql, params) in jobs.items()}
    return {k: f.result() for k, f in futs.items()}


# Tiny TTL cache for global, slow-changing payloads (overview KPIs, filter lists).
_cache = {}
def cached(key, ttl, fn):
    now = time.time()
    hit = _cache.get(key)
    if hit and now - hit[0] < ttl:
        return hit[1]
    val = fn()
    _cache[key] = (now, val)
    return val


app = FastAPI(title="MedTech Customer 360")


@app.get("/api/health")
def health():
    return {"ok": True, "catalog": CATALOG, "schema": SCHEMA, "mode": "app" if IS_APP else "local"}


@app.get("/api/filters")
def filters():
    def _impl():
        regions = [r["region"] for r in q("SELECT DISTINCT region FROM dim_account ORDER BY region")]
        tiers = [r["account_tier"] for r in q("SELECT DISTINCT account_tier FROM dim_account ORDER BY account_tier")]
        return {"regions": regions, "tiers": tiers,
                "risks": ["Healthy", "Watch", "At Risk"], "business_units": ALL_BUS}
    return cached("filters", 600, _impl)


@app.get("/api/overview")
def overview():
    def _impl():
        res = q_many({
            "kpi": ("""SELECT
              ROUND((SELECT SUM(net_amount_usd) FROM fact_orders WHERE year(order_date)=year(current_date()))/1e6,1) AS revenue_ytd_m,
              (SELECT COUNT(*) FROM dim_account) AS accounts,
              ROUND((SELECT SUM(amount_usd) FROM fact_opportunity WHERE is_closed=false)/1e6,1) AS pipeline_m,
              ROUND((SELECT 100.0*COUNT_IF(is_won)/NULLIF(COUNT_IF(is_closed),0) FROM fact_opportunity WHERE year(actual_close_date)=year(current_date())),1) AS win_rate_pct,
              (SELECT COUNT(*) FROM vw_account_360 WHERE churn_risk='At Risk') AS at_risk,
              (SELECT COUNT(*) FROM fact_complaint WHERE is_mdr=true AND resolution_status IN ('Open','Investigating')) AS open_mdr""", None),
            "by_bu": ("""SELECT business_unit, ROUND(SUM(net_revenue_usd),0) AS revenue
                         FROM vw_revenue_monthly WHERE year=year(current_date())
                         GROUP BY business_unit ORDER BY revenue DESC""", None),
        })
        return {"kpi": res["kpi"][0], "by_bu": res["by_bu"]}
    return cached("overview", 120, _impl)


@app.get("/api/accounts")
def accounts(query: str = "", region: str = "", tier: str = "", risk: str = "",
             sort: str = "revenue", limit: int = 40):
    lim = max(1, min(int(limit), 200))
    preds, params = [], {}
    if query:
        preds.append("lower(account_name) LIKE lower(:q)"); params["q"] = f"%{query}%"
    if region:
        preds.append("region = :region"); params["region"] = region
    if tier:
        preds.append("account_tier = :tier"); params["tier"] = tier
    if risk:
        preds.append("churn_risk = :risk"); params["risk"] = risk
    where = ("WHERE " + " AND ".join(preds)) if preds else ""
    order = {"revenue": "revenue_ytd_usd DESC", "lifetime": "lifetime_revenue_usd DESC",
             "risk": "days_since_last_order DESC NULLS LAST", "pipeline": "open_pipeline_usd DESC",
             "name": "account_name ASC"}.get(sort, "revenue_ytd_usd DESC")
    return q(f"""SELECT account_id, account_name, account_type, region, account_tier, churn_risk,
                   ROUND(revenue_ytd_usd,0) AS revenue_ytd, ROUND(open_pipeline_usd,0) AS open_pipeline,
                   days_since_last_order, open_complaints, whitespace_business_units
                 FROM vw_account_360 {where} ORDER BY {order} LIMIT {lim}""", params)


@app.get("/api/accounts/{account_id}")
def account_detail(account_id: str):
    if not re.match(r"^ACC-\d+$", account_id):
        raise HTTPException(400, "bad account id")
    pid = {"id": account_id}
    res = q_many({
        "hdr": ("SELECT * FROM vw_account_360 WHERE account_id = :id", pid),
        "by_bu": ("""SELECT p.business_unit, ROUND(SUM(o.net_amount_usd),0) AS revenue, COUNT(*) AS lines
                     FROM fact_orders o JOIN dim_product p USING (product_id)
                     WHERE o.account_id = :id GROUP BY p.business_unit ORDER BY revenue DESC""", pid),
        "trend": ("""SELECT date_format(date_trunc('MONTH', order_date),'yyyy-MM') AS ym,
                       ROUND(SUM(net_amount_usd),0) AS revenue
                     FROM fact_orders
                     WHERE account_id = :id AND order_date >= add_months(date_trunc('MONTH', DATE'2026-06-18'), -17)
                     GROUP BY 1 ORDER BY 1""", pid),
        "orders": ("""SELECT o.order_date, p.product_name, p.business_unit, o.quantity,
                        ROUND(o.net_amount_usd,0) AS net_amount, o.channel
                      FROM fact_orders o JOIN dim_product p USING (product_id)
                      WHERE o.account_id = :id ORDER BY o.order_date DESC LIMIT 20""", pid),
        "opps": ("""SELECT stage, business_unit, ROUND(amount_usd,0) AS amount, probability_pct, expected_close_date
                    FROM fact_opportunity WHERE account_id = :id AND is_closed=false
                    ORDER BY amount_usd DESC LIMIT 20""", pid),
        "complaints": ("""SELECT c.date_reported, p.product_name, c.complaint_category, c.severity,
                            c.is_mdr, c.resolution_status
                          FROM fact_complaint c JOIN dim_product p USING (product_id)
                          WHERE c.account_id = :id ORDER BY c.date_reported DESC LIMIT 20""", pid),
        "purchased": ("""SELECT DISTINCT p.business_unit FROM fact_orders o JOIN dim_product p USING (product_id)
                         WHERE o.account_id = :id""", pid),
    })
    hdr = res["hdr"]
    if not hdr:
        raise HTTPException(404, "account not found")
    by_bu, trend, orders = res["by_bu"], res["trend"], res["orders"]
    opps, complaints = res["opps"], res["complaints"]
    purchased = {r["business_unit"] for r in res["purchased"]}
    whitespace = [bu for bu in ALL_BUS if bu not in purchased]

    # Next-best-action recommendations (rule-based, demo-friendly)
    h = hdr[0]
    actions = []
    dslo = h.get("days_since_last_order")
    dslo = int(dslo) if dslo not in (None, "") else None
    if h.get("churn_risk") == "At Risk":
        actions.append({"kind": "risk", "title": "Re-engage — lapsed account",
                        "detail": f"No order in {dslo} days. Schedule a clinical business review."})
    elif h.get("churn_risk") == "Watch":
        actions.append({"kind": "watch", "title": "Watch — ordering is slowing",
                        "detail": f"Last order {dslo} days ago. Confirm reorder cadence."})
    if whitespace:
        actions.append({"kind": "cross", "title": f"Cross-sell — {whitespace[0]}",
                        "detail": "Whitespace business unit with no orders yet. " +
                                  (f"+{len(whitespace)-1} more." if len(whitespace) > 1 else "Introduce the portfolio.")})
    if float(h.get("open_pipeline_usd") or 0) > 0 and int(h.get("open_opps") or 0) > 0:
        actions.append({"kind": "pipe", "title": "Advance open pipeline",
                        "detail": f"{h.get('open_opps')} open opportunit{'y' if str(h.get('open_opps'))=='1' else 'ies'} worth ${float(h['open_pipeline_usd']):,.0f}."})
    if int(h.get("open_complaints") or 0) > 0:
        actions.append({"kind": "quality", "title": "Resolve open complaints",
                        "detail": f"{h.get('open_complaints')} open complaint(s)" +
                                  (f", {h.get('mdr_complaints')} MDR-reportable." if int(h.get('mdr_complaints') or 0) > 0 else ".")})

    return {"header": h, "by_bu": by_bu, "trend": trend, "orders": orders,
            "opportunities": opps, "complaints": complaints,
            "whitespace": whitespace, "actions": actions}


# ---- static frontend ----
_static = os.path.join(os.path.dirname(__file__), "static")
app.mount("/static", StaticFiles(directory=_static), name="static")


@app.get("/")
def index():
    return FileResponse(os.path.join(_static, "index.html"))
