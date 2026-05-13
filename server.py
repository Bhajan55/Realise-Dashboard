"""
JIVO Realise Dashboard — Standalone Server
FastAPI + SAP HANA via REPORT_SALES_ANALYSIS procedure
Drill-down uses cached data — no extra SAP calls
Historical avg realise with drill-level breakdowns
Targets stored in targets.json (legacy) / Supabase (v2)

FIX: targets.json is the single source of truth for all target values.
     DEFAULT_TARGETS is only a fallback when a key has never been saved.
     SAP data fetch never resets or overwrites saved targets.

SUPABASE (v2): monthly_targets table — per month/year/product.
     Falls back to DEFAULT_TARGETS when no row exists for a month/year/product.
     Full cutover: once Supabase is live, /api/targets-v2 and /api/save-targets-v2
     replace /api/targets and /api/save-targets respectively.
"""

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, Response
from pydantic import BaseModel
from typing import Optional, List
import json, os
from dotenv import load_dotenv
load_dotenv()
from datetime import datetime, date, timedelta
import csv
import io
import socket

try:
    from dateutil.relativedelta import relativedelta
except ImportError:
    class relativedelta:
        def __init__(self, months=0, days=0):
            self.months = months
            self.days = days
        def __rsub__(self, other):
            m = other.month - self.months
            y = other.year
            while m <= 0:
                m += 12
                y -= 1
            d = min(other.day, 28)
            result = date(y, m, d)
            if self.days:
                result = result - timedelta(days=self.days)
            return result

# ==================== CONFIG ====================
SAP_HANA_HOST     = os.environ.get("SAP_HANA_HOST", "103.89.45.192")
SAP_HANA_PORT     = int(os.environ.get("SAP_HANA_PORT", "30015"))
SAP_HANA_USER     = os.environ.get("SAP_HANA_USER", "DATA1")
SAP_HANA_PASSWORD = os.environ.get("SAP_HANA_PASSWORD", "Jivo@1989")
SAP_HANA_ENCRYPT  = os.environ.get("SAP_HANA_ENCRYPT", "").strip().lower()
SAP_HANA_SSL_VALIDATE_CERTIFICATE = os.environ.get("SAP_HANA_SSL_VALIDATE_CERTIFICATE", "").strip().lower()
SAP_CONNECT_TIMEOUT = float(os.environ.get("SAP_CONNECT_TIMEOUT", "8"))
SAP_SCHEMA        = os.environ.get("SAP_SCHEMA", "JIVO_OIL_HANADB")
SERVER_PORT       = int(os.environ.get("SERVER_PORT", "8002"))
TARGETS_FILE      = "targets.json"
CONFIG_FILE       = "config.json"

# ==================== SUPABASE CONFIG ====================
SUPABASE_URL      = os.environ.get("SUPABASE_URL", "")
SUPABASE_KEY      = os.environ.get("SUPABASE_KEY", "")

def get_supabase():
    """Return a Supabase client using the service role key (bypasses RLS)."""
    try:
        from supabase import create_client
        return create_client(SUPABASE_URL, SUPABASE_KEY)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Supabase client init failed: {str(e)}")

def load_config():
    if os.path.exists(CONFIG_FILE):
        with open(CONFIG_FILE, "r") as f:
            return json.load(f)
    return {"edit_pin": "1234"}

ALLOWED_SUB_GROUPS = [
    "BLENDED", "COTTON SEED", "MUSTARD", "RICE BRAN", "SLICED OLIVE",
    "SOYABEAN", "SUNFLOWER", "CANOLA", "COCONUT", "EXTRA VIRGIN OLIVE",
    "GHEE", "GROUNDNUT", "OLIVE", "SESAME", "YELLOW MUSTARD"
]

# DEFAULT_TARGETS: Fallback when no row exists in Supabase for a month/year/product.
# Keys: "TYPE|SUB_GROUP"  →  tgt_ltrs (formerly target_sale), tgt_rate (formerly target_realise)
DEFAULT_TARGETS = {
    "COMMODITY|BLENDED":          {"tgt_ltrs": 30000,   "tgt_rate": 130},
    "COMMODITY|COTTON SEED":      {"tgt_ltrs": 20000,   "tgt_rate": 131},
    "COMMODITY|MUSTARD":          {"tgt_ltrs": 625000,  "tgt_rate": 145},
    "COMMODITY|RICE BRAN":        {"tgt_ltrs": 25000,   "tgt_rate": 130},
    "PREMIUM|SLICED OLIVE":       {"tgt_ltrs": 0,       "tgt_rate": 0},
    "COMMODITY|SOYABEAN":         {"tgt_ltrs": 400000,  "tgt_rate": 123},
    "COMMODITY|SUNFLOWER":        {"tgt_ltrs": 135000,  "tgt_rate": 145},
    "PREMIUM|BLENDED":            {"tgt_ltrs": 10000,   "tgt_rate": 190},
    "PREMIUM|CANOLA":             {"tgt_ltrs": 350000,  "tgt_rate": 205},
    "PREMIUM|COCONUT":            {"tgt_ltrs": 5000,    "tgt_rate": 449},
    "PREMIUM|EXTRA VIRGIN OLIVE": {"tgt_ltrs": 10000,   "tgt_rate": 500},
    "PREMIUM|GHEE":               {"tgt_ltrs": 15000,   "tgt_rate": 536},
    "PREMIUM|GROUNDNUT":          {"tgt_ltrs": 50000,   "tgt_rate": 175},
    "PREMIUM|OLIVE":              {"tgt_ltrs": 310000,  "tgt_rate": 253},
    "PREMIUM|SESAME":             {"tgt_ltrs": 5000,    "tgt_rate": 0},
    "PREMIUM|YELLOW MUSTARD":     {"tgt_ltrs": 10000,   "tgt_rate": 180},
}

# ==================== APP ====================
app = FastAPI(title="JIVO Realise Dashboard", version="1.0")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

# ==================== CACHES ====================
_cache      = {"raw_data": [], "columns": [], "col_map": {}, "start_date": None, "end_date": None, "fetched_at": None}
_hist_cache = {"data": {}, "raw_data": [], "end_date": None, "fetched_at": None}

# ==================== LEGACY TARGETS (targets.json) ====================
def load_targets():
    if os.path.exists(TARGETS_FILE):
        with open(TARGETS_FILE, "r") as f:
            return json.load(f)
    return {}

def save_targets_file(data):
    with open(TARGETS_FILE, "w") as f:
        json.dump(data, f, indent=2)

def get_target_for_key(key: str, saved_targets: dict) -> dict:
    """
    Resolve targets for a TYPE|SUB_GROUP key.
    Priority: targets.json (saved) > DEFAULT_TARGETS > zeros
    Used by the legacy /api/sales-data flow until full Supabase cutover.
    """
    if key in saved_targets:
        saved = saved_targets[key]
        return {
            "target_sale": saved.get("target_sale", saved.get("tgt_ltrs", 0)),
            "target_realise": saved.get("target_realise", saved.get("tgt_rate", 0)),
        }
    if key in DEFAULT_TARGETS:
        d = DEFAULT_TARGETS[key]
        return {
            "target_sale": d.get("tgt_ltrs", d.get("target_sale", 0)),
            "target_realise": d.get("tgt_rate", d.get("target_realise", 0)),
        }
    return {"target_sale": 0, "target_realise": 0}

# ==================== MODELS ====================
class DateRange(BaseModel):
    start_date: str
    end_date: str
    period: Optional[str] = None

class TargetUpdate(BaseModel):
    key: str
    target_sale: float = 0
    target_realise: float = 0
    difference6: float = 0

class BulkTargetUpdate(BaseModel):
    targets: list[TargetUpdate]

class DrillDownRequest(BaseModel):
    start_date: str
    end_date: str
    u_type: str
    u_sub_group: str
    drill_by: str
    month: Optional[str] = None
    year: Optional[str] = None
    filters: Optional[dict] = None

class GroupDataRequest(BaseModel):
    start_date: str
    end_date: str
    group_by: str
    type_filter: Optional[str] = None
    month: Optional[str] = None
    year: Optional[str] = None
    products: Optional[List[str]] = None
    filters: Optional[dict] = None

class PinVerify(BaseModel):
    pin: str

class LoginRequest(BaseModel):
    username: str
    password: str

# ---- Supabase v2 models ----
class TargetUpdateV2(BaseModel):
    month: int                  # 1–12
    year: int                   # e.g. 2026
    product_name: str           # e.g. "CANOLA"
    product_type: str           # "PREMIUM" or "COMMODITY"
    tgt_ltrs: float = 0
    tgt_rate: float = 0
    updated_by: str             # username from session

class BulkTargetUpdateV2(BaseModel):
    targets: List[TargetUpdateV2]

class MigrateRequest(BaseModel):
    month: int                  # seed into this month (1–12)
    year: int                   # seed into this year
    updated_by: str             # who is running the migration

# ==================== SAP HANA ====================
def check_sap_tcp():
    started = datetime.now()
    try:
        with socket.create_connection((SAP_HANA_HOST, SAP_HANA_PORT), timeout=SAP_CONNECT_TIMEOUT):
            elapsed_ms = int((datetime.now() - started).total_seconds() * 1000)
            return {"ok": True, "host": SAP_HANA_HOST, "port": SAP_HANA_PORT, "elapsed_ms": elapsed_ms}
    except OSError as e:
        elapsed_ms = int((datetime.now() - started).total_seconds() * 1000)
        return {
            "ok": False,
            "host": SAP_HANA_HOST,
            "port": SAP_HANA_PORT,
            "elapsed_ms": elapsed_ms,
            "error": str(e),
        }

def get_sap_connection():
    try:
        from hdbcli import dbapi
        tcp = check_sap_tcp()
        if not tcp["ok"]:
            raise ConnectionError(
                f"Cannot reach SAP HANA TCP endpoint {SAP_HANA_HOST}:{SAP_HANA_PORT} "
                f"within {SAP_CONNECT_TIMEOUT:g}s: {tcp['error']}"
            )

        connect_kwargs = {
            "address": SAP_HANA_HOST,
            "port": SAP_HANA_PORT,
            "user": SAP_HANA_USER,
            "password": SAP_HANA_PASSWORD,
        }
        if SAP_HANA_ENCRYPT in ("true", "1", "yes", "y"):
            connect_kwargs["encrypt"] = True
        elif SAP_HANA_ENCRYPT in ("false", "0", "no", "n"):
            connect_kwargs["encrypt"] = False
        if SAP_HANA_SSL_VALIDATE_CERTIFICATE in ("true", "1", "yes", "y"):
            connect_kwargs["sslValidateCertificate"] = True
        elif SAP_HANA_SSL_VALIDATE_CERTIFICATE in ("false", "0", "no", "n"):
            connect_kwargs["sslValidateCertificate"] = False

        conn = dbapi.connect(**connect_kwargs)
        return conn
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"SAP HANA connection failed: {str(e)}")

# ==================== HELPERS ====================
def parse_doc_date(doc_date):
    if isinstance(doc_date, (datetime, date)):
        return doc_date.strftime("%b").upper(), str(doc_date.year)
    if isinstance(doc_date, str) and doc_date.strip():
        s = doc_date.strip()
        for fmt in ("%Y-%m-%d", "%d-%m-%Y", "%Y/%m/%d", "%d/%m/%Y", "%Y-%m-%d %H:%M:%S"):
            try:
                dt = datetime.strptime(s[:len(fmt.replace('%','').replace('d','00').replace('m','00').replace('Y','0000').replace('H','00').replace('M','00').replace('S','00'))], fmt)
                return dt.strftime("%b").upper(), str(dt.year)
            except:
                pass
        try:
            dt = datetime.fromisoformat(s[:19])
            return dt.strftime("%b").upper(), str(dt.year)
        except:
            pass
    return "", ""

RECLASSIFY_RULES = [
    ("YELLOW MUSTARD",       "PREMIUM", "YELLOW MUSTARD"),
    ("EXTRA VIRGIN COCONUT", "PREMIUM", "COCONUT"),
    ("EXTRA VIRGIN",         "PREMIUM", "EXTRA VIRGIN OLIVE"),
    ("SLICED OLIVE",         "PREMIUM", "SLICED OLIVE"),
]

def reclassify_item(u_type, u_sub, item_name):
    for keyword, new_type, new_sub in RECLASSIFY_RULES:
        if keyword in item_name or keyword in u_sub:
            return new_type, new_sub
    return u_type, u_sub

# ==================== ROUTES ====================
@app.get("/")
async def serve_dashboard():
    return FileResponse("dashboard.html", media_type="text/html")

@app.get("/dashboard.html")
async def serve_dashboard_html():
    return FileResponse("dashboard.html", media_type="text/html")

@app.get("/health")
async def health():
    tcp = check_sap_tcp()
    if not tcp["ok"]:
        return {"status": "error", "sap_connected": False, "tcp": tcp}
    try:
        conn = get_sap_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT CURRENT_TIMESTAMP FROM DUMMY")
        ts = cursor.fetchone()[0]
        cursor.close(); conn.close()
        return {"status": "ok", "sap_connected": True, "sap_time": str(ts), "tcp": tcp}
    except Exception as e:
        return {"status": "error", "sap_connected": False, "tcp": tcp, "error": str(e)}

@app.post("/api/verify-pin")
async def verify_pin(req: PinVerify):
    config = load_config()
    correct_pin = config.get("edit_pin", "1234")
    if req.pin == correct_pin:
        return {"status": "ok", "verified": True}
    else:
        return {"status": "error", "verified": False, "message": "Incorrect PIN"}

@app.post("/api/login")
async def login(req: LoginRequest):
    config = load_config()
    users = config.get("users", {})
    user = users.get(req.username.lower().strip())
    if not user:
        return {"status": "error", "message": "Invalid username"}
    if user.get("password") != req.password:
        return {"status": "error", "message": "Incorrect password"}
    return {
        "status": "ok",
        "username": req.username.lower().strip(),
        "role": user.get("role", "viewer"),
        "type_filter": user.get("type_filter", ""),
        "can_edit": user.get("can_edit", False),
        "display_name": user.get("display_name", req.username)
    }

@app.post("/api/sales-data")
async def get_sales_data(params: DateRange):
    conn = None
    try:
        conn = get_sap_connection()
        cursor = conn.cursor()
        start_dt = datetime.strptime(params.start_date, "%Y-%m-%d").date()
        end_dt   = datetime.strptime(params.end_date,   "%Y-%m-%d").date()

        cursor.execute(f'CALL "{SAP_SCHEMA}"."REPORT_SALES_ANALYSIS"(?, ?)', (start_dt, end_dt))
        columns  = [desc[0] for desc in cursor.description]
        rows_raw = cursor.fetchall()

        col_map = {}
        for i, c in enumerate(columns):
            col_map[c.upper()] = i
            col_map[c] = i

        raw_dicts = [dict(zip(columns, row)) for row in rows_raw]
        _cache["raw_data"]   = raw_dicts
        _cache["columns"]    = columns
        _cache["col_map"]    = col_map
        _cache["start_date"] = params.start_date
        _cache["end_date"]   = params.end_date
        _cache["fetched_at"] = datetime.now().isoformat()

        saved_targets = load_targets()

        grouped = {}

        for d in raw_dicts:
            u_type    = str(d.get("U_TYPE", "")).strip().upper()
            u_sub     = str(d.get("U_Sub_Group", "")).strip().upper()
            item_name = str(d.get("ItemName", "") or "").strip().upper()
            u_type, u_sub = reclassify_item(u_type, u_sub, item_name)

            if u_sub not in ALLOWED_SUB_GROUPS:
                continue

            litres    = float(d.get("Liter", 0) or 0)
            linetotal = float(d.get("LineTotal", 0) or 0)
            doc_date  = d.get("DocDate", "")
            month, year = parse_doc_date(doc_date)

            base_key  = f"{u_type}|{u_sub}"
            group_key = f"{u_type}|{u_sub}|{month}|{year}"

            if group_key not in grouped:
                t = get_target_for_key(base_key, saved_targets)
                grouped[group_key] = {
                    "u_type": u_type, "u_sub_group": u_sub, "month": month, "year": year,
                    "litres": 0, "linetotal": 0,
                    "target_sale": t["target_sale"],
                    "target_realise": t["target_realise"],
                    "row_key": group_key
                }
            grouped[group_key]["litres"]    += litres
            grouped[group_key]["linetotal"] += linetotal

        rows = []
        for gk, g in grouped.items():
            g["litres"]   = round(g["litres"], 2)
            g["linetotal"] = round(g["linetotal"], 2)
            g["realise"]  = round(g["linetotal"] / g["litres"], 2) if g["litres"] > 0 else 0
            rows.append(g)

        cursor.close(); conn.close()
        rows.sort(key=lambda x: (0 if x["u_type"] == "PREMIUM" else 1, -x.get("target_sale", 0), x["u_sub_group"], x["month"]))

        print(f"[SALES] {len(rows_raw)} raw → {len(rows)} grouped")

        try:
            if _hist_cache["end_date"] != params.end_date or not _hist_cache["data"]:
                fetch_historical_data(params.end_date)
        except Exception as he:
            print(f"[HIST] Background fetch failed: {he}")

        return {"status": "ok", "count": len(rows), "data": rows, "grouped_rows": len(rows)}
    except HTTPException:
        raise
    except Exception as e:
        import traceback; traceback.print_exc()
        return {"status": "error", "error": str(e), "data": []}
    finally:
        if conn:
            try: conn.close()
            except: pass

@app.get("/api/export-raw-csv")
async def export_raw_csv():
    raw_rows = _cache.get("raw_data") or []
    columns  = _cache.get("columns") or []
    if not raw_rows or not columns:
        raise HTTPException(status_code=400, detail="No raw data available — click Fetch Data first")

    buf    = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(columns)

    for row in raw_rows:
        values = []
        for col in columns:
            val = row.get(col, "")
            if isinstance(val, (datetime, date)):
                val = val.isoformat()
            values.append(val)
        writer.writerow(values)

    start_date = _cache.get("start_date") or "from"
    end_date   = _cache.get("end_date")   or "to"
    filename   = f"Sales_RAW_{start_date}_{end_date}.csv"
    return Response(
        content=buf.getvalue(),
        media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'}
    )

# ==================== HISTORICAL ====================
def fetch_historical_data(end_date_str):
    conn = None
    try:
        conn     = get_sap_connection()
        cursor   = conn.cursor()
        end_dt   = datetime.strptime(end_date_str, "%Y-%m-%d").date()
        start_dt = end_dt - relativedelta(months=12)
        print(f"[HIST] Fetching: {start_dt} to {end_dt}")

        cursor.execute(f'CALL "{SAP_SCHEMA}"."REPORT_SALES_ANALYSIS"(?, ?)', (start_dt, end_dt))
        columns  = [desc[0] for desc in cursor.description]
        rows_raw = cursor.fetchall()
        raw_dicts = [dict(zip(columns, row)) for row in rows_raw]

        hist = {}
        for d in raw_dicts:
            u_type    = str(d.get("U_TYPE", "")).strip().upper()
            u_sub     = str(d.get("U_Sub_Group", "")).strip().upper()
            item_name = str(d.get("ItemName", "") or "").strip().upper()
            u_type, u_sub = reclassify_item(u_type, u_sub, item_name)
            if u_sub not in ALLOWED_SUB_GROUPS: continue

            litres    = float(d.get("Liter", 0) or 0)
            linetotal = float(d.get("LineTotal", 0) or 0)
            month, year = parse_doc_date(d.get("DocDate", ""))
            if not month or not year: continue

            key = f"{u_type}|{u_sub}|{month}|{year}"
            if key not in hist:
                hist[key] = {"u_type": u_type, "u_sub_group": u_sub, "month": month, "year": year, "litres": 0, "linetotal": 0}
            hist[key]["litres"]    += litres
            hist[key]["linetotal"] += linetotal

        _hist_cache["data"]      = hist
        _hist_cache["raw_data"]  = raw_dicts
        _hist_cache["end_date"]  = end_date_str
        _hist_cache["fetched_at"] = datetime.now().isoformat()
        print(f"[HIST] Cached {len(hist)} groups from {len(rows_raw)} rows")
        cursor.close(); conn.close()
    except Exception as e:
        import traceback; traceback.print_exc()
        print(f"[HIST] Error: {e}")
    finally:
        if conn:
            try: conn.close()
            except: pass

@app.post("/api/historical-realise")
async def get_historical_realise(params: DateRange):
    end_dt = datetime.strptime(params.end_date, "%Y-%m-%d").date()
    period = getattr(params, 'period', '12m') or '12m'

    if _hist_cache["end_date"] != params.end_date or not _hist_cache["data"]:
        fetch_historical_data(params.end_date)

    if period == '12m':       start_dt = end_dt - relativedelta(months=12)
    elif period == '6m':      start_dt = end_dt - relativedelta(months=6)
    elif period == '3m':      start_dt = end_dt - relativedelta(months=3)
    elif period == 'last_month':
        first_of_current = end_dt.replace(day=1)
        last_of_prev     = first_of_current - relativedelta(days=1)
        start_dt         = last_of_prev.replace(day=1)
        end_dt           = last_of_prev
    else: start_dt = end_dt - relativedelta(months=12)

    MONTHS_ORDER = ['JAN','FEB','MAR','APR','MAY','JUN','JUL','AUG','SEP','OCT','NOV','DEC']
    DRILL_COLS   = ["State", "U_Main_Group", "U_Chain", "ItemName", "CardName"]

    agg       = {}
    drill_agg = {}

    for d in _hist_cache.get("raw_data", []):
        m, y = parse_doc_date(d.get("DocDate", ""))
        if not m or not y: continue
        try:
            month_idx = MONTHS_ORDER.index(m)
            row_date  = date(int(y), month_idx + 1, 1)
        except: continue

        if row_date < start_dt.replace(day=1) or row_date > end_dt: continue

        u_type    = str(d.get("U_TYPE", "")).strip().upper()
        u_sub     = str(d.get("U_Sub_Group", "")).strip().upper()
        item_name = str(d.get("ItemName", "") or "").strip().upper()
        u_type, u_sub = reclassify_item(u_type, u_sub, item_name)
        if u_sub not in ALLOWED_SUB_GROUPS: continue

        litres    = float(d.get("Liter", 0) or 0)
        linetotal = float(d.get("LineTotal", 0) or 0)

        pk = f"{u_type}|{u_sub}"
        if pk not in agg: agg[pk] = {"litres": 0, "linetotal": 0}
        agg[pk]["litres"]    += litres
        agg[pk]["linetotal"] += linetotal

        for dc in DRILL_COLS:
            dim_val = str(d.get(dc, "") or "").strip().upper()
            if not dim_val: continue
            dk = f"{pk}|{dc}|{dim_val}"
            if dk not in drill_agg: drill_agg[dk] = {"litres": 0, "linetotal": 0}
            drill_agg[dk]["litres"]    += litres
            drill_agg[dk]["linetotal"] += linetotal

    result       = {pk: round(v["linetotal"] / v["litres"], 2) if v["litres"] > 0 else 0 for pk, v in agg.items()}
    drill_result = {dk: round(v["linetotal"] / v["litres"], 2) if v["litres"] > 0 else 0 for dk, v in drill_agg.items()}

    print(f"[HIST] Period={period} | {start_dt} to {end_dt} | {len(result)} products, {len(drill_result)} drill combos")
    return {"status": "ok", "data": result, "drill_data": drill_result, "period": period}

# ==================== DRILL-DOWN ====================
@app.post("/api/drill-down")
async def drill_down(req: DrillDownRequest):
    if not _cache["raw_data"]:
        raise HTTPException(400, "No cached data — click Fetch Data first")

    columns   = _cache["columns"]
    drill_col = None
    for c in columns:
        if c.upper() == req.drill_by.upper() or c == req.drill_by:
            drill_col = c; break

    if not drill_col:
        raise HTTPException(400, f"Column '{req.drill_by}' not found. Available: {columns}")

    results = {}
    for d in _cache["raw_data"]:
        u_type    = str(d.get("U_TYPE", "")).strip().upper()
        u_sub     = str(d.get("U_Sub_Group", "")).strip().upper()
        item_name = str(d.get("ItemName", "") or "").strip().upper()
        u_type, u_sub = reclassify_item(u_type, u_sub, item_name)
        product_key = f"{u_type}|{u_sub}"

        if u_type != req.u_type.upper() or u_sub != req.u_sub_group.upper(): continue

        if req.month or req.year:
            m, y = parse_doc_date(d.get("DocDate", ""))
            if req.month and m != req.month: continue
            if req.year  and y != req.year:  continue

        if req.filters:
            skip = False
            for fk, fv in req.filters.items():
                if fk.upper() in ("U_TYPE", "TYPE"):
                    if u_type != str(fv).upper():
                        skip = True
                        break
                    continue
                if fk.upper() in ("U_SUB_GROUP", "PRODUCT"):
                    if product_key != str(fv).upper() and u_sub != str(fv).upper():
                        skip = True
                        break
                    continue
                val = ""
                for c in columns:
                    if c.upper() == fk.upper() or c == fk:
                        val = str(d.get(c, "")).strip(); break
                if val.upper() != str(fv).upper(): skip = True; break
            if skip: continue

        dim_val = str(d.get(drill_col, "") or "UNKNOWN").strip()
        if not dim_val: dim_val = "UNKNOWN"
        litres    = float(d.get("Liter", 0) or 0)
        linetotal = float(d.get("LineTotal", 0) or 0)

        if dim_val not in results:
            results[dim_val] = {"dimension": dim_val, "litres": 0, "linetotal": 0}
        results[dim_val]["litres"]    += litres
        results[dim_val]["linetotal"] += linetotal

    data = sorted(results.values(), key=lambda x: x["litres"], reverse=True)
    return {"data": data}

@app.post("/api/group-data")
async def group_data(req: GroupDataRequest):
    if not _cache["raw_data"]:
        raise HTTPException(400, "No cached data — click Fetch Data first")

    columns = _cache["columns"]
    group_by = (req.group_by or "Product").strip()
    group_col = None
    if group_by.upper() != "PRODUCT":
        for c in columns:
            if c.upper() == group_by.upper() or c == group_by:
                group_col = c
                break
        if not group_col:
            raise HTTPException(400, f"Column '{req.group_by}' not found. Available: {columns}")

    selected_products = set()
    for p in req.products or []:
        if "|" in p:
            selected_products.add(p.strip().upper())
    if req.products is not None and not selected_products:
        return {"status": "ok", "data": []}

    results = {}
    for d in _cache["raw_data"]:
        u_type    = str(d.get("U_TYPE", "")).strip().upper()
        u_sub     = str(d.get("U_Sub_Group", "")).strip().upper()
        item_name = str(d.get("ItemName", "") or "").strip().upper()
        u_type, u_sub = reclassify_item(u_type, u_sub, item_name)

        if u_sub not in ALLOWED_SUB_GROUPS:
            continue
        if req.type_filter and u_type != req.type_filter.upper():
            continue
        product_key = f"{u_type}|{u_sub}"
        if selected_products and product_key not in selected_products:
            continue

        if req.month or req.year:
            m, y = parse_doc_date(d.get("DocDate", ""))
            if req.month and m != req.month:
                continue
            if req.year and y != req.year:
                continue

        if req.filters:
            skip = False
            for fk, fv in req.filters.items():
                if fk.upper() in ("U_TYPE", "TYPE"):
                    if u_type != str(fv).upper():
                        skip = True
                        break
                    continue
                if fk.upper() == "PRODUCT":
                    if product_key != str(fv).upper():
                        skip = True
                        break
                    continue
                val = ""
                for c in columns:
                    if c.upper() == fk.upper() or c == fk:
                        val = str(d.get(c, "")).strip()
                        break
                if val.upper() != str(fv).upper():
                    skip = True
                    break
            if skip:
                continue

        if group_by.upper() == "PRODUCT":
            dim_val = product_key
            label = u_sub
            result_key = dim_val
        else:
            dim_val = str(d.get(group_col, "") or "UNKNOWN").strip() or "UNKNOWN"
            label = dim_val
            result_key = f"{u_type}|{dim_val}"

        litres    = float(d.get("Liter", 0) or 0)
        linetotal = float(d.get("LineTotal", 0) or 0)

        if result_key not in results:
            results[result_key] = {
                "dimension": label,
                "value": dim_val,
                "group_by": group_by,
                "u_type": u_type,
                "u_sub_group": u_sub if group_by.upper() == "PRODUCT" else "",
                "litres": 0,
                "linetotal": 0,
            }
        results[result_key]["litres"]    += litres
        results[result_key]["linetotal"] += linetotal

    data = []
    for row in results.values():
        row["litres"] = round(row["litres"], 2)
        row["linetotal"] = round(row["linetotal"], 2)
        row["realise"] = round(row["linetotal"] / row["litres"], 2) if row["litres"] > 0 else 0
        data.append(row)
    data.sort(key=lambda x: (0 if x["u_type"] == "PREMIUM" else 1, -x["litres"], x["dimension"]))
    return {"status": "ok", "data": data}

# ==================== LEGACY TARGETS (targets.json) ====================
# Keep these routes intact until full Supabase cutover.

@app.post("/api/save-targets")
async def save_targets(params: BulkTargetUpdate):
    try:
        targets = load_targets()
        for t in params.targets:
            targets[t.key] = {"target_sale": t.target_sale, "target_realise": t.target_realise, "difference6": t.difference6}
            print(f"[SAVE] {t.key} → sale={t.target_sale}, realise={t.target_realise}")
        save_targets_file(targets)
        print(f"[SAVE] Written {len(params.targets)} targets to {TARGETS_FILE}")
        return {"status": "ok", "saved": len(params.targets)}
    except Exception as e:
        print(f"[SAVE] ERROR: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/targets")
async def get_targets():
    """
    Returns merged targets: saved targets.json values merged over DEFAULT_TARGETS.
    Legacy endpoint — kept for backward compatibility until Supabase cutover.
    """
    saved  = load_targets()
    merged = {}
    for key, defaults in DEFAULT_TARGETS.items():
        if key in saved:
            merged[key] = saved[key]
        else:
            merged[key] = defaults
    for key, val in saved.items():
        if key not in merged:
            merged[key] = val
    return merged

# ==================== SUPABASE TARGETS (v2) ====================

@app.get("/api/targets-v2")
async def get_targets_v2(month: int, year: int):
    """
    Fetch targets for a specific month/year from Supabase.
    For any product not found in Supabase, falls back to DEFAULT_TARGETS.

    Response shape (keyed by "TYPE|PRODUCT_NAME"):
    {
      "PREMIUM|CANOLA":   { "tgt_ltrs": 350000, "tgt_rate": 225, "source": "supabase" },
      "COMMODITY|MUSTARD":{ "tgt_ltrs": 625000, "tgt_rate": 145, "source": "default"  },
      ...
    }
    """
    try:
        sb = get_supabase()

        # Fetch all rows for this month/year
        response = (
            sb.table("monthly_targets")
            .select("product_name, product_type, tgt_ltrs, tgt_rate")
            .eq("month", month)
            .eq("year", year)
            .execute()
        )

        # Build lookup from Supabase rows
        sb_lookup = {}
        for row in (response.data or []):
            key = f"{row['product_type'].upper()}|{row['product_name'].upper()}"
            sb_lookup[key] = {
                "tgt_ltrs": float(row["tgt_ltrs"]),
                "tgt_rate":  float(row["tgt_rate"]),
                "source":    "supabase",
            }

        # Merge with DEFAULT_TARGETS — Supabase rows take priority
        merged = {}
        for key, defaults in DEFAULT_TARGETS.items():
            if key in sb_lookup:
                merged[key] = sb_lookup[key]
            else:
                merged[key] = {
                    "tgt_ltrs": defaults["tgt_ltrs"],
                    "tgt_rate":  defaults["tgt_rate"],
                    "source":    "default",
                }

        # Include any Supabase rows for products not in DEFAULT_TARGETS
        for key, val in sb_lookup.items():
            if key not in merged:
                merged[key] = val

        print(f"[TGT-V2] GET month={month} year={year} → {len(sb_lookup)} from Supabase, {len(merged)} total")
        return {"status": "ok", "month": month, "year": year, "data": merged}

    except HTTPException:
        raise
    except Exception as e:
        import traceback; traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/save-targets-v2")
async def save_targets_v2(params: BulkTargetUpdateV2):
    """
    Upsert targets into Supabase monthly_targets table.
    Uses UNIQUE constraint (month, year, product_name) — existing rows are overwritten.
    Admin gate: enforce can_edit=True in the frontend before calling this endpoint.
    """
    try:
        sb = get_supabase()

        rows_to_upsert = []
        now_iso = datetime.utcnow().isoformat()
        existing_lookup = {}

        for t in params.targets:
            product_name = t.product_name.strip().upper()
            product_type = t.product_type.strip().upper()
            existing_resp = (
                sb.table("monthly_targets")
                .select("id, tgt_ltrs, tgt_rate")
                .eq("month", t.month)
                .eq("year", t.year)
                .eq("product_name", product_name)
                .limit(1)
                .execute()
            )
            existing_row = (existing_resp.data or [None])[0]
            existing_lookup[f"{t.month}|{t.year}|{product_name}"] = existing_row

            rows_to_upsert.append({
                "month":        t.month,
                "year":         t.year,
                "product_name": product_name,
                "product_type": product_type,
                "tgt_ltrs":     t.tgt_ltrs,
                "tgt_rate":     t.tgt_rate,
                "updated_by":   t.updated_by,
                "updated_at":   now_iso,
                # created_by / created_at only written on first insert via upsert
                "created_by":   t.updated_by,
                "created_at":   now_iso,
            })

        # Manual insert/update — avoids needing a DB-level UNIQUE constraint
        for row in rows_to_upsert:
            key = f"{row['month']}|{row['year']}|{row['product_name']}"
            existing = existing_lookup.get(key)
            if existing:
                (
                    sb.table("monthly_targets")
                    .update({
                        "tgt_ltrs":   row["tgt_ltrs"],
                        "tgt_rate":   row["tgt_rate"],
                        "updated_by": row["updated_by"],
                        "updated_at": row["updated_at"],
                    })
                    .eq("id", existing["id"])
                    .execute()
                )
            else:
                (
                    sb.table("monthly_targets")
                    .insert(row)
                    .execute()
                )

        # Best-effort audit logging. Save should not fail if audit table/schema differs.
        try:
            audit_rows = []
            for t in params.targets:
                product_name = t.product_name.strip().upper()
                key = f"{t.month}|{t.year}|{product_name}"
                old_row = existing_lookup.get(key)
                old_values = None
                monthly_target_id = None
                if old_row:
                    monthly_target_id = old_row.get("id")
                    old_values = {
                        "tgt_ltrs": float(old_row.get("tgt_ltrs", 0) or 0),
                        "tgt_rate": float(old_row.get("tgt_rate", 0) or 0),
                    }
                audit_rows.append({
                    "monthly_target_id": monthly_target_id,
                    "month": t.month,
                    "year": t.year,
                    "product_name": product_name,
                    "product_type": t.product_type.strip().upper(),
                    "old_values": old_values,
                    "new_values": {"tgt_ltrs": t.tgt_ltrs, "tgt_rate": t.tgt_rate},
                    "changed_by": t.updated_by,
                    "changed_at": now_iso,
                })
            if audit_rows:
                sb.table("monthly_target_audit").insert(audit_rows).execute()
        except Exception as audit_err:
            print(f"[TGT-V2] AUDIT WARN: {audit_err}")

        print(f"[TGT-V2] SAVE month/year mix → {len(rows_to_upsert)} rows upserted by {params.targets[0].updated_by if params.targets else '?'}")
        return {"status": "ok", "saved": len(rows_to_upsert)}

    except HTTPException:
        raise
    except Exception as e:
        import traceback; traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/migrate-targets")
async def migrate_targets(req: MigrateRequest):
    """
    ONE-TIME migration endpoint.
    Reads current targets.json and seeds all products into Supabase
    for the specified month/year.

    Steps:
      1. Call POST /api/migrate-targets with { month, year, updated_by }
      2. Verify data in Supabase table
      3. Remove or disable this endpoint

    NOTE: Will not overwrite rows that already exist for the same month/year/product
    unless you change the on_conflict behaviour below.
    """
    try:
        saved = load_targets()
        if not saved:
            # Fall back to DEFAULT_TARGETS if targets.json is empty
            saved = {
                key: {"target_sale": v["tgt_ltrs"], "target_realise": v["tgt_rate"]}
                for key, v in DEFAULT_TARGETS.items()
            }

        sb      = get_supabase()
        now_iso = datetime.utcnow().isoformat()
        rows    = []

        for key, val in saved.items():
            parts = key.split("|", 1)
            if len(parts) != 2:
                print(f"[MIGRATE] Skipping malformed key: {key}")
                continue
            product_type, product_name = parts

            # Support both old field names (target_sale/target_realise) and new (tgt_ltrs/tgt_rate)
            tgt_ltrs = float(val.get("tgt_ltrs", val.get("target_sale", 0)))
            tgt_rate  = float(val.get("tgt_rate",  val.get("target_realise", 0)))

            rows.append({
                "month":        req.month,
                "year":         req.year,
                "product_name": product_name.strip().upper(),
                "product_type": product_type.strip().upper(),
                "tgt_ltrs":     tgt_ltrs,
                "tgt_rate":     tgt_rate,
                "created_by":   req.updated_by,
                "created_at":   now_iso,
                "updated_by":   req.updated_by,
                "updated_at":   now_iso,
            })

        # Fetch existing product names for this month/year to avoid duplicate inserts
        existing_resp = (
            sb.table("monthly_targets")
            .select("product_name")
            .eq("month", req.month)
            .eq("year", req.year)
            .execute()
        )
        existing_names = {r["product_name"] for r in (existing_resp.data or [])}
        rows_to_insert = [r for r in rows if r["product_name"] not in existing_names]

        if rows_to_insert:
            sb.table("monthly_targets").insert(rows_to_insert).execute()

        print(f"[MIGRATE] Seeded {len(rows_to_insert)} products (skipped {len(rows) - len(rows_to_insert)} existing) → month={req.month} year={req.year} by {req.updated_by}")
        return {
            "status":  "ok",
            "seeded":  len(rows),
            "month":   req.month,
            "year":    req.year,
            "message": "Migration complete. Disable this endpoint after verifying data in Supabase."
        }

    except HTTPException:
        raise
    except Exception as e:
        import traceback; traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


# ==================== RUN ====================
if __name__ == "__main__":
    import uvicorn
    print(f"\n🚀 JIVO Realise Dashboard at http://localhost:{SERVER_PORT}\n")
    uvicorn.run(app, host="localhost", port=SERVER_PORT)
