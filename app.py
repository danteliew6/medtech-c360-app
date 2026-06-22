"""
MedTech Customer 360 — sample Databricks App (FastAPI), backed by Lakebase Autoscaling.

Reads the C360 serving layer (schema `medtech`) from a Lakebase Autoscaling project via
psycopg, using short-lived OAuth tokens (refreshed before the 1-hour expiry) as the password.
The Autoscaling endpoint scales to zero after 5 min idle and wakes on the next connection;
q() retries transparently so the first request after idle just pays a short wake penalty.
Thread-local connections keep the q_many() parallel fan-out fast. Dual-mode auth: injected
service principal in Databricks Apps, CLI profile locally.
"""
import os
import re
import time
import threading
from concurrent.futures import ThreadPoolExecutor
from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
import psycopg
from psycopg.rows import dict_row
from databricks.sdk import WorkspaceClient
from pydantic import BaseModel
from typing import Optional

PROJECT = os.environ.get("LAKEBASE_PROJECT", "medtech-c360")
BRANCH = os.environ.get("LAKEBASE_BRANCH", "production")
ENDPOINT = f"projects/{PROJECT}/branches/{BRANCH}/endpoints/primary"
DBNAME = os.environ.get("LAKEBASE_DB", "databricks_postgres")
IS_APP = bool(os.environ.get("DATABRICKS_APP_NAME"))
ALL_BUS = ["Interventional Systems", "Blood & Cell Technologies", "Cardiovascular",
           "Medical Care Solutions", "Diabetes Care"]

_w = None
def w():
    global _w
    if _w is None:
        _w = WorkspaceClient() if IS_APP else WorkspaceClient(profile=os.environ.get("DATABRICKS_PROFILE", "fevm-dante-classic-stable"))
    return _w

# ---- connection management: cached token (refresh < 1h) + thread-local connections ----
_host = None
_user = None
_token = None
_token_ts = 0.0
_tlock = threading.Lock()
_tls = threading.local()

def _conn_params():
    global _host, _user, _token, _token_ts
    with _tlock:
        if _host is None:
            ep = w().postgres.get_endpoint(name=ENDPOINT)
            _host = ep.status.hosts.host
            _user = w().current_user.me().user_name
        if not _token or (time.time() - _token_ts) > 2700:  # refresh every 45 min
            _token = w().postgres.generate_database_credential(endpoint=ENDPOINT).token
            _token_ts = time.time()
        return _host, _user, _token

def _conn():
    host, user, token = _conn_params()
    c = getattr(_tls, "conn", None)
    if c is not None and not c.closed and getattr(_tls, "tok", None) == token:
        return c
    if c is not None:
        try: c.close()
        except Exception: pass
    # connect_timeout high enough to absorb a scale-to-zero wake (~2-5s)
    c = psycopg.connect(host=host, dbname=DBNAME, user=user, password=token,
                        sslmode="require", connect_timeout=30, autocommit=True,
                        row_factory=dict_row)
    _tls.conn = c
    _tls.tok = token
    return c

def q(sql, params=None):
    # retry-on-wake: the autoscale endpoint may be asleep or drop a stale conn;
    # the first attempt triggers the wake, the retry runs once it's up.
    last = None
    for attempt in range(3):
        try:
            with _conn().cursor() as cur:
                cur.execute(sql, params or {})
                return cur.fetchall() if cur.description else []
        except psycopg.Error as e:
            last = e
            try: _tls.conn.close()
            except Exception: pass
            _tls.conn = None
            if attempt < 2:
                time.sleep(1.5 * (attempt + 1))  # brief backoff while the DB wakes
    raise HTTPException(500, f"db error: {str(last)[:200]}")

_EXEC = ThreadPoolExecutor(max_workers=8)
def q_many(jobs):
    futs = {k: _EXEC.submit(q, sql, params) for k, (sql, params) in jobs.items()}
    return {k: f.result() for k, f in futs.items()}

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


@app.on_event("startup")
def _warm():
    # open a connection on each executor thread up front so the first real
    # request doesn't pay cold TLS/token setup across the parallel fan-out
    try:
        q_many({f"w{i}": ("SELECT 1 AS ok", None) for i in range(8)})
    except Exception:
        pass


@app.get("/api/health")
def health():
    backend = "lakebase"
    try:
        q("SELECT 1 AS ok")
    except Exception as e:
        return {"ok": False, "backend": backend, "error": str(e)[:200]}
    return {"ok": True, "backend": backend, "project": PROJECT, "db": DBNAME,
            "mode": "app" if IS_APP else "local"}


@app.get("/api/filters")
def filters():
    def _impl():
        regions = [r["region"] for r in q("SELECT DISTINCT region FROM medtech.account_360 ORDER BY region")]
        tiers = [r["account_tier"] for r in q("SELECT DISTINCT account_tier FROM medtech.account_360 ORDER BY account_tier")]
        return {"regions": regions, "tiers": tiers,
                "risks": ["Healthy", "Watch", "At Risk"], "business_units": ALL_BUS}
    return cached("filters", 600, _impl)


@app.get("/api/overview")
def overview():
    def _impl():
        res = q_many({
            "kpi": ("""SELECT
              (round(((SELECT SUM(net_amount_usd) FROM medtech.orders WHERE order_year=2026)/1e6)::numeric,1))::float8 AS revenue_ytd_m,
              (SELECT COUNT(*) FROM medtech.account_360) AS accounts,
              (round(((SELECT SUM(amount_usd) FROM medtech.opportunities WHERE NOT is_closed)/1e6)::numeric,1))::float8 AS pipeline_m,
              (SELECT (round((100.0*COUNT(*) FILTER (WHERE is_won)/NULLIF(COUNT(*) FILTER (WHERE is_closed),0))::numeric,1))::float8
                 FROM medtech.opportunities WHERE actual_close_year=2026) AS win_rate_pct,
              (SELECT COUNT(*) FROM medtech.account_360 WHERE churn_risk='At Risk') AS at_risk,
              (SELECT COUNT(*) FROM medtech.complaints WHERE is_mdr AND resolution_status IN ('Open','Investigating')) AS open_mdr""", None),
            "by_bu": ("""SELECT business_unit, round(SUM(net_amount_usd))::float8 AS revenue
                         FROM medtech.orders WHERE order_year=2026
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
        preds.append("account_name ILIKE %(q)s"); params["q"] = f"%{query}%"
    if region:
        preds.append("region = %(region)s"); params["region"] = region
    if tier:
        preds.append("account_tier = %(tier)s"); params["tier"] = tier
    if risk:
        preds.append("churn_risk = %(risk)s"); params["risk"] = risk
    where = ("WHERE " + " AND ".join(preds)) if preds else ""
    order = {"revenue": "revenue_ytd_usd DESC", "lifetime": "lifetime_revenue_usd DESC",
             "risk": "days_since_last_order DESC NULLS LAST", "pipeline": "open_pipeline_usd DESC",
             "name": "account_name ASC"}.get(sort, "revenue_ytd_usd DESC")
    return q(f"""SELECT account_id, account_name, account_type, region, account_tier, churn_risk,
                   round(revenue_ytd_usd)::float8 AS revenue_ytd, round(open_pipeline_usd)::float8 AS open_pipeline,
                   days_since_last_order, open_complaints, whitespace_business_units
                 FROM medtech.account_360 {where} ORDER BY {order} LIMIT {lim}""", params)


@app.get("/api/accounts/{account_id}")
def account_detail(account_id: str):
    if not re.match(r"^ACC-\d+$", account_id):
        raise HTTPException(400, "bad account id")
    p = {"id": account_id}
    res = q_many({
        "hdr": ("SELECT * FROM medtech.account_360 WHERE account_id = %(id)s", p),
        "by_bu": ("""SELECT business_unit, round(SUM(net_amount_usd))::float8 AS revenue, COUNT(*) AS lines
                     FROM medtech.orders WHERE account_id = %(id)s GROUP BY business_unit ORDER BY revenue DESC""", p),
        "trend": ("""SELECT to_char(order_month,'YYYY-MM') AS ym, round(SUM(net_amount_usd))::float8 AS revenue
                     FROM medtech.orders
                     WHERE account_id = %(id)s AND order_month >= DATE '2026-06-01' - INTERVAL '17 month'
                     GROUP BY order_month ORDER BY order_month""", p),
        "orders": ("""SELECT order_date, product_name, business_unit, quantity,
                        round(net_amount_usd)::float8 AS net_amount, channel
                      FROM medtech.orders WHERE account_id = %(id)s ORDER BY order_date DESC LIMIT 20""", p),
        "opps": ("""SELECT stage, business_unit, round(amount_usd)::float8 AS amount, probability_pct, expected_close_date
                    FROM medtech.opportunities WHERE account_id = %(id)s AND NOT is_closed
                    ORDER BY amount_usd DESC LIMIT 20""", p),
        "complaints": ("""SELECT date_reported, product_name, complaint_category, severity, is_mdr, resolution_status
                          FROM medtech.complaints WHERE account_id = %(id)s ORDER BY date_reported DESC LIMIT 20""", p),
        "purchased": ("SELECT DISTINCT business_unit FROM medtech.orders WHERE account_id = %(id)s", p),
        "logged": ("""SELECT action_id, action_type, title, detail, status, due_date,
                        to_char(created_at,'YYYY-MM-DD') AS created_at,
                        to_char(completed_at,'YYYY-MM-DD') AS completed_at
                      FROM medtech.account_actions WHERE account_id = %(id)s
                      ORDER BY status='done', created_at DESC LIMIT 50""", p),
        "activity": ("""SELECT * FROM (
                          SELECT order_date::text AS ts, 'order' AS kind, product_name AS label,
                                 business_unit AS meta, round(net_amount_usd)::float8 AS amount
                          FROM medtech.orders WHERE account_id = %(id)s
                          UNION ALL
                          SELECT expected_close_date::text, 'opportunity', stage, business_unit, round(amount_usd)::float8
                          FROM medtech.opportunities WHERE account_id = %(id)s AND NOT is_closed
                          UNION ALL
                          SELECT date_reported::text, 'complaint', complaint_category, severity, NULL
                          FROM medtech.complaints WHERE account_id = %(id)s
                          UNION ALL
                          SELECT created_at::text, 'action', title, action_type, NULL
                          FROM medtech.account_actions WHERE account_id = %(id)s
                        ) t WHERE ts IS NOT NULL ORDER BY ts DESC LIMIT 30""", p),
    })
    hdr = res["hdr"]
    if not hdr:
        raise HTTPException(404, "account not found")
    h = hdr[0]
    by_bu, trend, orders = res["by_bu"], res["trend"], res["orders"]
    opps, complaints = res["opps"], res["complaints"]
    purchased = {r["business_unit"] for r in res["purchased"]}
    whitespace = [bu for bu in ALL_BUS if bu not in purchased]

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
            "whitespace": whitespace, "actions": actions,
            "logged_actions": res["logged"], "activity": res["activity"]}


# ---- write-back: rep actions persisted to Lakebase (the app does OLTP, not just reads) ----
class ActionIn(BaseModel):
    action_type: str = "note"
    title: str
    detail: Optional[str] = None
    due_date: Optional[str] = None


@app.post("/api/accounts/{account_id}/actions")
def create_action(account_id: str, body: ActionIn):
    if not re.match(r"^ACC-\d+$", account_id):
        raise HTTPException(400, "bad account id")
    title = (body.title or "").strip()
    if not title:
        raise HTTPException(400, "title required")
    who = "app" if IS_APP else (w().current_user.me().user_name or "local")
    row = q("""INSERT INTO medtech.account_actions
                 (account_id, action_type, title, detail, due_date, created_by)
               VALUES (%(a)s, %(t)s, %(ti)s, %(d)s, %(due)s, %(by)s)
               RETURNING action_id, action_type, title, detail, status, due_date,
                         to_char(created_at,'YYYY-MM-DD') AS created_at""",
            {"a": account_id, "t": body.action_type[:40], "ti": title[:200],
             "d": (body.detail or None), "due": (body.due_date or None), "by": who[:120]})
    return row[0] if row else {}


@app.post("/api/actions/{action_id}/toggle")
def toggle_action(action_id: int):
    row = q("""UPDATE medtech.account_actions
               SET status = CASE WHEN status='open' THEN 'done' ELSE 'open' END,
                   completed_at = CASE WHEN status='open' THEN now() ELSE NULL END
               WHERE action_id = %(id)s
               RETURNING action_id, status""", {"id": int(action_id)})
    if not row:
        raise HTTPException(404, "action not found")
    return row[0]


@app.get("/api/actions")
def list_actions(status: str = "open", limit: int = 100):
    lim = max(1, min(int(limit), 500))
    where = "WHERE a.status = %(s)s" if status in ("open", "done") else ""
    params = {"s": status} if where else {}
    return q(f"""SELECT a.action_id, a.account_id, ac.account_name, a.action_type, a.title,
                   a.detail, a.status, a.due_date, to_char(a.created_at,'YYYY-MM-DD') AS created_at
                 FROM medtech.account_actions a
                 JOIN medtech.account_360 ac USING (account_id)
                 {where} ORDER BY a.created_at DESC LIMIT {lim}""", params)


# ---- static frontend ----
_static = os.path.join(os.path.dirname(__file__), "static")
app.mount("/static", StaticFiles(directory=_static), name="static")


@app.get("/")
def index():
    return FileResponse(os.path.join(_static, "index.html"))
