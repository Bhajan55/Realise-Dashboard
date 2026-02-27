"""
JIVO Realise Dashboard — Standalone Server
FastAPI + SAP HANA via REPORT_SALES_ANALYSIS procedure
Drill-down uses cached data — no extra SAP calls
Historical avg realise with drill-level breakdowns
Targets stored in targets.json
"""

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from pydantic import BaseModel
from typing import Optional
import json, os
from datetime import datetime, date, timedelta

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
SAP_HANA_HOST = "103.89.45.192"
SAP_HANA_PORT = 30015
SAP_HANA_USER = "DATA1"
SAP_HANA_PASSWORD = "Jivo@8912"
SAP_SCHEMA = "JIVO_OIL_HANADB"
SERVER_PORT = 8002
TARGETS_FILE = "targets.json"

ALLOWED_SUB_GROUPS = [
    "BLENDED", "COTTON SEED", "MUSTARD", "RICE BRAN", "SLICED OLIVE",
    "SOYABEAN", "SUNFLOWER", "CANOLA", "COCONUT", "EXTRA VIRGIN OLIVE",
    "GHEE", "GROUNDNUT", "OLIVE", "SESAME", "YELLOW MUSTARD"
]

DEFAULT_TARGETS = {
    "COMMODITY|BLENDED":          {"target_sale": 30000,  "target_realise": 130},
    "COMMODITY|COTTON SEED":      {"target_sale": 20000,  "target_realise": 131},
    "COMMODITY|MUSTARD":          {"target_sale": 625000, "target_realise": 145},
    "COMMODITY|RICE BRAN":        {"target_sale": 25000,  "target_realise": 130},
    "PREMIUM|SLICED OLIVE":       {"target_sale": 0,      "target_realise": 0},
    "COMMODITY|SOYABEAN":         {"target_sale": 400000, "target_realise": 123},
    "COMMODITY|SUNFLOWER":        {"target_sale": 135000, "target_realise": 145},
    "PREMIUM|BLENDED":            {"target_sale": 10000,  "target_realise": 190},
    "PREMIUM|CANOLA":             {"target_sale": 350000, "target_realise": 205},
    "PREMIUM|COCONUT":            {"target_sale": 5000,   "target_realise": 449},
    "PREMIUM|EXTRA VIRGIN OLIVE": {"target_sale": 10000,  "target_realise": 500},
    "PREMIUM|GHEE":               {"target_sale": 15000,  "target_realise": 536},
    "PREMIUM|GROUNDNUT":          {"target_sale": 50000,  "target_realise": 175},
    "PREMIUM|OLIVE":              {"target_sale": 310000, "target_realise": 253},
    "PREMIUM|SESAME":             {"target_sale": 5000,   "target_realise": 0},
    "PREMIUM|YELLOW MUSTARD":     {"target_sale": 10000,  "target_realise": 180},
}

# ==================== APP ====================
app = FastAPI(title="JIVO Realise Dashboard", version="1.0")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

# ==================== CACHES ====================
_cache = {"raw_data": [], "columns": [], "col_map": {}, "start_date": None, "end_date": None, "fetched_at": None}
_hist_cache = {"data": {}, "raw_data": [], "end_date": None, "fetched_at": None}

# ==================== TARGETS ====================
def load_targets():
    if os.path.exists(TARGETS_FILE):
        with open(TARGETS_FILE, "r") as f:
            return json.load(f)
    return {}

def save_targets_file(data):
    with open(TARGETS_FILE, "w") as f:
        json.dump(data, f, indent=2)

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

# ==================== SAP HANA ====================
def get_sap_connection():
    try:
        from hdbcli import dbapi
        conn = dbapi.connect(address=SAP_HANA_HOST, port=SAP_HANA_PORT, user=SAP_HANA_USER, password=SAP_HANA_PASSWORD)
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
    ("YELLOW MUSTARD", "PREMIUM", "YELLOW MUSTARD"),
    ("EXTRA VIRGIN",   "PREMIUM", "EXTRA VIRGIN OLIVE"),
    ("SLICED OLIVE",   "PREMIUM", "SLICED OLIVE"),
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

@app.get("/health")
async def health():
    try:
        conn = get_sap_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT CURRENT_TIMESTAMP FROM DUMMY")
        ts = cursor.fetchone()[0]
        cursor.close(); conn.close()
        return {"status": "ok", "sap_connected": True, "sap_time": str(ts)}
    except Exception as e:
        return {"status": "error", "sap_connected": False, "error": str(e)}

@app.post("/api/sales-data")
async def get_sales_data(params: DateRange):
    conn = None
    try:
        conn = get_sap_connection()
        cursor = conn.cursor()
        start_dt = datetime.strptime(params.start_date, "%Y-%m-%d").date()
        end_dt = datetime.strptime(params.end_date, "%Y-%m-%d").date()

        cursor.execute(f'CALL "{SAP_SCHEMA}"."REPORT_SALES_ANALYSIS"(?, ?)', (start_dt, end_dt))
        columns = [desc[0] for desc in cursor.description]
        rows_raw = cursor.fetchall()

        col_map = {}
        for i, c in enumerate(columns):
            col_map[c.upper()] = i
            col_map[c] = i

        raw_dicts = [dict(zip(columns, row)) for row in rows_raw]
        _cache["raw_data"] = raw_dicts
        _cache["columns"] = columns
        _cache["col_map"] = col_map
        _cache["start_date"] = params.start_date
        _cache["end_date"] = params.end_date
        _cache["fetched_at"] = datetime.now().isoformat()

        targets = load_targets()
        grouped = {}

        for d in raw_dicts:
            u_type = str(d.get("U_TYPE", "")).strip().upper()
            u_sub = str(d.get("U_Sub_Group", "")).strip().upper()
            item_name = str(d.get("ItemName", "") or "").strip().upper()
            u_type, u_sub = reclassify_item(u_type, u_sub, item_name)

            if u_sub not in ALLOWED_SUB_GROUPS:
                continue

            litres = float(d.get("Liter", 0) or 0)
            linetotal = float(d.get("LineTotal", 0) or 0)
            doc_date = d.get("DocDate", "")
            month, year = parse_doc_date(doc_date)

            base_key = f"{u_type}|{u_sub}"
            group_key = f"{u_type}|{u_sub}|{month}|{year}"

            if group_key not in grouped:
                # Simple: always look up by TYPE|SUB
                saved = targets.get(base_key, {})
                defaults = DEFAULT_TARGETS.get(base_key, {"target_sale": 0, "target_realise": 0})
                ts = saved["target_sale"] if "target_sale" in saved else defaults["target_sale"]
                tr = saved["target_realise"] if "target_realise" in saved else defaults["target_realise"]
                grouped[group_key] = {
                    "u_type": u_type, "u_sub_group": u_sub, "month": month, "year": year,
                    "litres": 0, "linetotal": 0,
                    "target_sale": ts,
                    "target_realise": tr,
                    "row_key": group_key
                }
            grouped[group_key]["litres"] += litres
            grouped[group_key]["linetotal"] += linetotal

        rows = []
        for gk, g in grouped.items():
            g["litres"] = round(g["litres"], 2)
            g["linetotal"] = round(g["linetotal"], 2)
            g["realise"] = round(g["linetotal"] / g["litres"], 2) if g["litres"] > 0 else 0
            rows.append(g)

        cursor.close(); conn.close()
        rows.sort(key=lambda x: (x["u_type"], x["u_sub_group"], x["month"]))

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

# ==================== HISTORICAL ====================
def fetch_historical_data(end_date_str):
    conn = None
    try:
        conn = get_sap_connection()
        cursor = conn.cursor()
        end_dt = datetime.strptime(end_date_str, "%Y-%m-%d").date()
        start_dt = end_dt - relativedelta(months=12)
        print(f"[HIST] Fetching: {start_dt} to {end_dt}")

        cursor.execute(f'CALL "{SAP_SCHEMA}"."REPORT_SALES_ANALYSIS"(?, ?)', (start_dt, end_dt))
        columns = [desc[0] for desc in cursor.description]
        rows_raw = cursor.fetchall()
        raw_dicts = [dict(zip(columns, row)) for row in rows_raw]

        hist = {}
        for d in raw_dicts:
            u_type = str(d.get("U_TYPE", "")).strip().upper()
            u_sub = str(d.get("U_Sub_Group", "")).strip().upper()
            item_name = str(d.get("ItemName", "") or "").strip().upper()
            u_type, u_sub = reclassify_item(u_type, u_sub, item_name)
            if u_sub not in ALLOWED_SUB_GROUPS: continue

            litres = float(d.get("Liter", 0) or 0)
            linetotal = float(d.get("LineTotal", 0) or 0)
            month, year = parse_doc_date(d.get("DocDate", ""))
            if not month or not year: continue

            key = f"{u_type}|{u_sub}|{month}|{year}"
            if key not in hist:
                hist[key] = {"u_type": u_type, "u_sub_group": u_sub, "month": month, "year": year, "litres": 0, "linetotal": 0}
            hist[key]["litres"] += litres
            hist[key]["linetotal"] += linetotal

        _hist_cache["data"] = hist
        _hist_cache["raw_data"] = raw_dicts
        _hist_cache["end_date"] = end_date_str
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

    if period == '12m':    start_dt = end_dt - relativedelta(months=12)
    elif period == '6m':   start_dt = end_dt - relativedelta(months=6)
    elif period == '3m':   start_dt = end_dt - relativedelta(months=3)
    elif period == 'last_month':
        first_of_current = end_dt.replace(day=1)
        last_of_prev = first_of_current - relativedelta(days=1)
        start_dt = last_of_prev.replace(day=1)
        end_dt = last_of_prev
    else: start_dt = end_dt - relativedelta(months=12)

    MONTHS_ORDER = ['JAN','FEB','MAR','APR','MAY','JUN','JUL','AUG','SEP','OCT','NOV','DEC']
    DRILL_COLS = ["State", "U_Main_Group", "U_Chain", "ItemName"]

    agg = {}
    drill_agg = {}

    for d in _hist_cache.get("raw_data", []):
        m, y = parse_doc_date(d.get("DocDate", ""))
        if not m or not y: continue
        try:
            month_idx = MONTHS_ORDER.index(m)
            row_date = date(int(y), month_idx + 1, 1)
        except: continue

        if row_date < start_dt.replace(day=1) or row_date > end_dt: continue

        u_type = str(d.get("U_TYPE", "")).strip().upper()
        u_sub = str(d.get("U_Sub_Group", "")).strip().upper()
        item_name = str(d.get("ItemName", "") or "").strip().upper()
        u_type, u_sub = reclassify_item(u_type, u_sub, item_name)
        if u_sub not in ALLOWED_SUB_GROUPS: continue

        litres = float(d.get("Liter", 0) or 0)
        linetotal = float(d.get("LineTotal", 0) or 0)

        pk = f"{u_type}|{u_sub}"
        if pk not in agg: agg[pk] = {"litres": 0, "linetotal": 0}
        agg[pk]["litres"] += litres
        agg[pk]["linetotal"] += linetotal

        for dc in DRILL_COLS:
            dim_val = str(d.get(dc, "") or "").strip().upper()
            if not dim_val: continue
            dk = f"{pk}|{dc}|{dim_val}"
            if dk not in drill_agg: drill_agg[dk] = {"litres": 0, "linetotal": 0}
            drill_agg[dk]["litres"] += litres
            drill_agg[dk]["linetotal"] += linetotal

    result = {pk: round(v["linetotal"] / v["litres"], 2) if v["litres"] > 0 else 0 for pk, v in agg.items()}
    drill_result = {dk: round(v["linetotal"] / v["litres"], 2) if v["litres"] > 0 else 0 for dk, v in drill_agg.items()}

    print(f"[HIST] Period={period} | {start_dt} to {end_dt} | {len(result)} products, {len(drill_result)} drill combos")
    return {"status": "ok", "data": result, "drill_data": drill_result, "period": period}

# ==================== DRILL-DOWN ====================
@app.post("/api/drill-down")
async def drill_down(req: DrillDownRequest):
    if not _cache["raw_data"]:
        raise HTTPException(400, "No cached data — click Fetch Data first")

    columns = _cache["columns"]
    drill_col = None
    for c in columns:
        if c.upper() == req.drill_by.upper() or c == req.drill_by:
            drill_col = c; break

    if not drill_col:
        raise HTTPException(400, f"Column '{req.drill_by}' not found. Available: {columns}")

    results = {}
    for d in _cache["raw_data"]:
        u_type = str(d.get("U_TYPE", "")).strip().upper()
        u_sub = str(d.get("U_Sub_Group", "")).strip().upper()
        item_name = str(d.get("ItemName", "") or "").strip().upper()
        u_type, u_sub = reclassify_item(u_type, u_sub, item_name)

        if u_type != req.u_type.upper() or u_sub != req.u_sub_group.upper(): continue

        if req.month or req.year:
            m, y = parse_doc_date(d.get("DocDate", ""))
            if req.month and m != req.month: continue
            if req.year and y != req.year: continue

        if req.filters:
            skip = False
            for fk, fv in req.filters.items():
                val = ""
                for c in columns:
                    if c.upper() == fk.upper() or c == fk:
                        val = str(d.get(c, "")).strip(); break
                if val.upper() != str(fv).upper(): skip = True; break
            if skip: continue

        dim_val = str(d.get(drill_col, "") or "UNKNOWN").strip()
        if not dim_val: dim_val = "UNKNOWN"
        litres = float(d.get("Liter", 0) or 0)
        linetotal = float(d.get("LineTotal", 0) or 0)

        if dim_val not in results:
            results[dim_val] = {"dimension": dim_val, "litres": 0, "linetotal": 0}
        results[dim_val]["litres"] += litres
        results[dim_val]["linetotal"] += linetotal

    data = sorted(results.values(), key=lambda x: x["litres"], reverse=True)
    return {"data": data}

# ==================== TARGETS ====================
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
    return load_targets()

# ==================== RUN ====================
if __name__ == "__main__":
    import uvicorn
    print(f"\n🚀 JIVO Realise Dashboard at http://localhost:{SERVER_PORT}\n")
    uvicorn.run(app, host="0.0.0.0", port=SERVER_PORT)
