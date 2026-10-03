import datetime
import hashlib
import json
import logging
import os
import re
import threading
import time
from __future__ import annotations

import asyncio
import datetime
import hashlib
import json
import logging
import math
import os
import re
import threading
import time
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from typing import Any, Optional, TypedDict

import psycopg2          # kept for background scrape thread
import requests
import sqlglot
import sqlglot.expressions as exp
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from langchain_core.messages import HumanMessage, SystemMessage
from langchain_core.prompts import PromptTemplate
from langchain_ollama import ChatOllama, OllamaLLM
from langgraph.graph import END, StateGraph
from prometheus_client import Gauge, start_http_server
from pydantic import BaseModel
from dataclasses import dataclass, field
from typing import Any, TypedDict

import psycopg2
import requests
from flask import Flask, jsonify, request
from flask_cors import CORS
from langchain_community.utilities import SQLDatabase
from langchain_core.messages import HumanMessage, SystemMessage
from langchain_ollama import ChatOllama, OllamaLLM
from langchain_core.prompts import PromptTemplate
from langgraph.graph import END, StateGraph
from prometheus_client import Gauge, start_http_server

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)
try:
    import asyncpg
    _ASYNCPG_AVAILABLE = True
except ImportError:
    _ASYNCPG_AVAILABLE = False

try:
    import redis as _redis_mod
    _REDIS_AVAILABLE = True
except ImportError:
    _REDIS_AVAILABLE = False

try:
    import numpy as np
    _NUMPY_AVAILABLE = True
except ImportError:
    _NUMPY_AVAILABLE = False
OLLAMA_HOST = os.getenv(
    "OLLAMA_HOST",
    "http://ollama:11434"
)



PG_HOST = os.getenv("PG_HOST", "localhost")
PG_PORT = os.getenv("PG_PORT", "5432")
PG_DB   = os.getenv("PG_DB",   "postgres")
PG_USER = os.getenv("PG_USER", "postgres")
PG_PASS = os.getenv("PG_PASS", "password")
DATABASE_URL     = os.getenv("DATABASE_URL", f"postgresql://{PG_USER}:{PG_PASS}@{PG_HOST}:{PG_PORT}/{PG_DB}")
DATABASE_URL_DSN = os.getenv("DATABASE_URL", f"postgresql://{PG_USER}:{PG_PASS}@{PG_HOST}:{PG_PORT}/{PG_DB}")

MAX_RETRIES         = int(os.getenv("MAX_RETRIES",       "3"))
EXPORTER_PORT       = int(os.getenv("EXPORTER_PORT",     "9877"))
SCRAPE_INTERVAL     = int(os.getenv("SCRAPE_INTERVAL",   "30"))
MAX_QUERY_COST      = float(os.getenv("MAX_QUERY_COST",  "500000"))
MAX_QUERY_ROWS      = int(os.getenv("MAX_QUERY_ROWS",    "1000000"))

GRAFANA_URL         = os.getenv("GRAFANA_URL",        "http://localhost:3000")
GRAFANA_USER        = os.getenv("GRAFANA_USER",       "admin")
GRAFANA_PASS        = os.getenv("GRAFANA_PASS",       "admin")
GRAFANA_API_KEY     = os.getenv("GRAFANA_API_KEY",    "")
PROMETHEUS_DS_NAME  = os.getenv("PROMETHEUS_DS_NAME", "Prometheus")

OLLAMA_MODEL        = os.getenv("OLLAMA_MODEL",        "llama3")
OLLAMA_EMBED_MODEL  = os.getenv("OLLAMA_EMBED_MODEL",  "nomic-embed-text")
FASTAPI_PORT        = int(os.getenv("FASTAPI_PORT",    "5050"))
REDIS_URL           = os.getenv("REDIS_URL",           "redis://localhost:6379/0")
CACHE_TTL           = int(os.getenv("CACHE_TTL",       "3600"))
DASHBOARD_SCORE_MIN = int(os.getenv("DASHBOARD_SCORE_MIN", "80"))
MAX_REGEN_ATTEMPTS  = int(os.getenv("MAX_REGEN_ATTEMPTS",  "2"))

FORBIDDEN_SQL = [
    "DELETE", "DROP", "TRUNCATE", "UPDATE",
    "INSERT", "ALTER", "CREATE", "GRANT",
    "REVOKE", "VACUUM", "REINDEX",
]

HIGH_CARDINALITY_COLS = {
    "id", "user_id", "order_id", "product_id", "customer_id",
    "email", "username", "uuid", "token",
    "created_at", "updated_at", "timestamp", "date",
}

DATETIME_COL_PATTERNS = re.compile(
    r"\b(date|time|timestamp|created_at|updated_at|occurred_at|event_time"
    r"|recorded_at|month|year|week|day|hour|period|quarter|dt)\b",
    re.IGNORECASE,
)

chat_llm = ChatOllama(model=OLLAMA_MODEL, temperature=0)
llm      = OllamaLLM(model=OLLAMA_MODEL, temperature=0)

_schema_cache:      str   = ""
_schema_cache_time: float = 0.0
_schema_lock               = threading.Lock()
SCHEMA_TTL                 = 300


def _fetch_schema_from_db() -> str:
    conn = None
    try:
        conn = _pg_connect()
        with conn.cursor() as cur:
            cur.execute("""
                SELECT table_schema, table_name, column_name, data_type
                FROM information_schema.columns
                WHERE table_schema NOT IN ('pg_catalog','information_schema')
                ORDER BY table_schema, table_name, ordinal_position
            """)
            rows = cur.fetchall()

        if not rows:
            return "No user tables found."

        tables: dict[str, list[str]] = {}
        for schema_name, table_name, col_name, dtype in rows:
            key = f"{schema_name}.{table_name}"
            tables.setdefault(key, []).append(f"{col_name} ({dtype})")

        lines = []
        for tname, cols in tables.items():
            lines.append(f"Table {tname}:")
            for c in cols:
                lines.append(f"  - {c}")
        return "\n".join(lines)
    finally:
        if conn:
            conn.close()


def get_schema() -> str:
    global _schema_cache, _schema_cache_time
    with _schema_lock:
        now = time.time()
        if not _schema_cache or (now - _schema_cache_time) > SCHEMA_TTL:
            log.info("[schema] Refreshing schema cache …")
            _schema_cache      = _fetch_schema_from_db()
            _schema_cache_time = now
        return _schema_cache

def _pg_connect():
    return psycopg2.connect(DATABASE_URL, options="-c statement_timeout=10000")


def execute_query_psycopg2(conn, sql: str) -> tuple[list[str], list[tuple]]:
    with conn.cursor() as cur:
        cur.execute(sql)
        col_names = [desc[0] for desc in cur.description] if cur.description else []
        rows = cur.fetchall()
    return col_names, rows

_sql_db:      "SQLDatabase | None" = None
_sql_db_lock  = threading.Lock()


def get_sql_db() -> "SQLDatabase":
    global _sql_db
    with _sql_db_lock:
        if _sql_db is None:
            _sql_db = SQLDatabase.from_uri(DATABASE_URL)
        return _sql_db


class SQLState(TypedDict):
    question: str
    schema:   str
    plan:     str
    sql:      str
    error:    str
    rows:     list
    retries:  int
    done:     bool


def _call_llm(system: str, user: str) -> str:
    return chat_llm.invoke(
        [SystemMessage(content=system), HumanMessage(content=user)]
    ).content.strip()


def _extract_sql(text: str) -> str:
    m = re.search(r"```sql\s*(.*?)```", text, re.DOTALL | re.IGNORECASE)
    if m:
        return m.group(1).strip()
    m = re.search(r"(WITH\s.+|SELECT\s.+)", text, re.DOTALL | re.IGNORECASE)
    return m.group(1).strip() if m else text.strip()


def _node_reason(state: SQLState) -> SQLState:
    schema = get_schema()
    plan = _call_llm(
        system=(
            "You are a PostgreSQL SQL planning expert.\n"
            "You will be given a database schema listing EXACT table and column names.\n"
            "Write a numbered step-by-step plan for building the SQL query.\n"
            "Reference ONLY table names and column names that appear in the schema.\n"
            "Do NOT write SQL yet — only the plan."
        ),
        user=(
            f"Question: {state['question']}\n\n"
            f"DATABASE SCHEMA (use ONLY these tables/columns):\n{schema}"
        ),
    )
    log.debug("[reason]\n%s", plan)
    return {**state, "schema": schema, "plan": plan}


def _node_generate(state: SQLState) -> SQLState:
    sql_raw = _call_llm(
        system=(
            "You are a PostgreSQL SQL expert.\n"
            "RULES:\n"
            "  1. Use ONLY the table names listed in the DATABASE SCHEMA below.\n"
            "  2. Use ONLY the column names listed in the DATABASE SCHEMA below.\n"
            "  3. Preserve exact schema-qualified names (e.g. sales.orders, not Orders).\n"
            "  4. Never invent table or column names.\n"
            "  5. Write a single, complete PostgreSQL SELECT query.\n"
            "  6. Add LIMIT 500 unless the question asks for a specific count.\n"
            "  7. Return ONLY the SQL inside a ```sql block. No explanation."
        ),
        user=(
            f"Question: {state['question']}\n\n"
            f"DATABASE SCHEMA (use ONLY these exact names):\n{state['schema']}\n\n"
            f"Plan:\n{state['plan']}"
        ),
    )
    sql = _extract_sql(sql_raw)
    log.debug("[generate]\n%s", sql)
    return {**state, "sql": sql, "error": ""}


def _node_validate(state: SQLState) -> SQLState:
    upper = state["sql"].upper()
    for kw in FORBIDDEN_SQL:
        if re.search(rf"\b{kw}\b", upper):
            return {**state, "error": f"Dangerous SQL keyword detected: {kw}"}

    review = _call_llm(
        system=(
            "You are a PostgreSQL SQL reviewer.\n"
            "Check the SQL against the schema for:\n"
            "  - wrong table names (must match schema exactly)\n"
            "  - wrong column names\n"
            "  - invalid aliases or dot-notation\n"
            "  - missing JOINs\n"
            "  - syntax errors\n"
            "Reply ONLY with 'OK' if correct, or one short sentence describing the problem."
        ),
        user=(
            f"DATABASE SCHEMA:\n{state['schema']}\n\n"
            f"SQL:\n{state['sql']}"
        ),
    )
    log.info("[validate] LLM review: %s", review)
    if review.strip().upper() != "OK":
        return {**state, "error": review}

    try:
        get_sql_db().run(f"EXPLAIN {state['sql']}")
        return {**state, "error": ""}
    except Exception as exc:
        return {**state, "error": str(exc)}


def _node_improve(state: SQLState) -> SQLState:
    fixed_raw = _call_llm(
        system=(
            "You are a PostgreSQL SQL repair expert.\n"
            "Fix the SQL based on the error.\n"
            "RULES:\n"
            "  1. Use ONLY table and column names from the DATABASE SCHEMA.\n"
            "  2. Preserve schema-qualified names exactly.\n"
            "  3. Return ONLY the corrected SQL inside a ```sql block."
        ),
        user=(
            f"Question: {state['question']}\n\n"
            f"DATABASE SCHEMA:\n{state['schema']}\n\n"
            f"Broken SQL:\n{state['sql']}\n\n"
            f"Error:\n{state['error']}"
        ),
    )
    fixed   = _extract_sql(fixed_raw)
    retries = state["retries"] + 1
    log.info("[improve] attempt %d\n%s", retries, fixed)
    return {**state, "sql": fixed, "retries": retries}


def _node_execute(state: SQLState) -> SQLState:
    try:
        result = get_sql_db().run(state["sql"])
        return {**state, "rows": result, "done": True}
    except Exception as exc:
        return {**state, "error": str(exc), "done": False}


def _route_after_validate(state: SQLState) -> str:
    if state["error"]:
        if state["retries"] >= MAX_RETRIES:
            return "fail_end"
        return "improve"
    return "execute"


def _build_graph() -> "StateGraph":
    g = StateGraph(SQLState)
    g.add_node("reason",    _node_reason)
    g.add_node("generate",  _node_generate)
    g.add_node("validate",  _node_validate)
    g.add_node("improve",   _node_improve)
    g.add_node("execute",   _node_execute)
    g.add_node("fail_end",  lambda s: {**s, "done": False})

    g.set_entry_point("reason")
    g.add_edge("reason",    "generate")
    g.add_edge("generate",  "validate")
    g.add_conditional_edges(
        "validate",
        _route_after_validate,
        {"improve": "improve", "execute": "execute", "fail_end": "fail_end"},
    )
    g.add_edge("improve",   "validate")
    g.add_edge("execute",   END)
    g.add_edge("fail_end",  END)
    return g


sql_pipeline = _build_graph().compile()


def run_sql_pipeline(question: str) -> SQLState:
    return sql_pipeline.invoke({
        "question": question,
        "schema":   "",
        "plan":     "",
        "sql":      "",
        "error":    "",
        "rows":     [],
        "retries":  0,
        "done":     False,
    })

_METRIC_VALUE_GAUGE: Optional[Gauge] = None
_UP_GAUGE:           Optional[Gauge] = None
_g_lock = threading.Lock()


def _get_metric_value_gauge() -> Gauge:
    global _METRIC_VALUE_GAUGE
    with _g_lock:
        if _METRIC_VALUE_GAUGE is None:
            _METRIC_VALUE_GAUGE = Gauge(
                "nlpg_metric_value",
                "NL→PG generic metric value",
                ["query_id", "metric_name", "domain", *["label_" + str(i) for i in range(4)]],
            )
        return _METRIC_VALUE_GAUGE


def _get_up_gauge() -> Gauge:
    global _UP_GAUGE
    with _g_lock:
        if _UP_GAUGE is None:
            _UP_GAUGE = Gauge(
                "nlpg_query_up",
                "1 if last NL→PG scrape succeeded",
                ["query_id", "title"],
            )
        return _UP_GAUGE


def _sanitize(name: str) -> str:
    s = re.sub(r"[^a-zA-Z0-9_]", "_", str(name).lower())
    s = re.sub(r"_+", "_", s).strip("_")
    return ("col_" + s) if (s and s[0].isdigit()) else (s or "value")


def _is_numeric(value: Any) -> bool:
    try:
        float(value)
        return True
    except (TypeError, ValueError):
        return False


def _classify_columns(rows, col_names):
    if not rows:
        return [], []
    sample = rows[0]
    num_cols = [col_names[i] for i, v in enumerate(sample) if _is_numeric(v)]
    str_cols = [
        col_names[i] for i, v in enumerate(sample)
        if not _is_numeric(v) and col_names[i].lower() not in HIGH_CARDINALITY_COLS
    ]
    return num_cols, str_cols


def push_metrics(rq: "RegisteredQuery", col_names: list[str], rows: list[tuple]):
    gauge = _get_metric_value_gauge()
    up_g  = _get_up_gauge()
    for num_col, metric_name in zip(rq.num_cols, rq.metric_names):
        num_idx  = col_names.index(num_col)
        str_idxs = [col_names.index(c) for c in rq.str_cols if c in col_names]
        for row in rows:
            raw_labels = [str(row[i]) if row[i] is not None else "" for i in str_idxs]
            label_vals = (raw_labels + ["", "", "", ""])[:4]
            try:
                val = float(row[num_idx]) if row[num_idx] is not None else 0.0
            except (TypeError, ValueError):
                val = 0.0
            gauge.labels(
                query_id=rq.query_id, metric_name=metric_name, domain=rq.domain,
                label_0=label_vals[0], label_1=label_vals[1],
                label_2=label_vals[2], label_3=label_vals[3],
            ).set(val)
    up_g.labels(query_id=rq.query_id, title=rq.title[:64]).set(1)


# ═══════════════════════════════════════════════════════════════════════════════
#  DOMAIN TEMPLATES + INTENT CLASSIFICATION
# ═══════════════════════════════════════════════════════════════════════════════
DOMAIN_TEMPLATES: dict[str, dict] = {
    "sales": {
        "title_prefix": "Sales Analytics",
        "primary_panel": "timeseries",
        "secondary_panels": ["stat", "barchart"],
        "kpi_labels": ["Total Revenue", "Orders", "Avg Order Value", "Top Category"],
        "color_scheme": "green",
        "suggested_unit": "currencyUSD",
    },
    "inventory": {
        "title_prefix": "Inventory Dashboard",
        "primary_panel": "bargauge",
        "secondary_panels": ["stat", "table"],
        "kpi_labels": ["Stock Level", "Low Stock Items", "Reorder Required"],
        "color_scheme": "orange",
        "suggested_unit": "short",
    },
    "finance": {
        "title_prefix": "Financial Overview",
        "primary_panel": "timeseries",
        "secondary_panels": ["stat", "barchart"],
        "kpi_labels": ["Revenue", "Expenses", "Net Profit", "Margin %"],
        "color_scheme": "green",
        "suggested_unit": "currencyUSD",
    },
    "monitoring": {
        "title_prefix": "System Monitoring",
        "primary_panel": "timeseries",
        "secondary_panels": ["stat", "bargauge"],
        "kpi_labels": ["Uptime", "Error Rate", "Latency P99", "Throughput"],
        "color_scheme": "red",
        "suggested_unit": "ms",
    },
    "customer": {
        "title_prefix": "Customer Analytics",
        "primary_panel": "barchart",
        "secondary_panels": ["stat", "table"],
        "kpi_labels": ["Active Users", "Churn Rate", "LTV", "Segments"],
        "color_scheme": "blue",
        "suggested_unit": "short",
    },
    "operations": {
        "title_prefix": "Operations Dashboard",
        "primary_panel": "table",
        "secondary_panels": ["stat", "barchart"],
        "kpi_labels": ["Processed", "Pending", "Failed", "SLA Compliance"],
        "color_scheme": "purple",
        "suggested_unit": "short",
    },
}

INTENT_KEYWORDS: dict[str, list[str]] = {
    "sales":       ["revenue", "sales", "order", "purchase", "gmv", "arr", "mrr", "transaction", "invoice"],
    "inventory":   ["stock", "inventory", "warehouse", "sku", "quantity", "reorder", "shelf", "supply"],
    "finance":     ["profit", "expense", "cost", "margin", "budget", "forecast", "loss", "balance", "cash"],
    "monitoring":  ["error", "latency", "uptime", "cpu", "memory", "disk", "alert", "incident", "p99"],
    "customer":    ["customer", "user", "churn", "ltv", "segment", "cohort", "retention", "nps"],
    "operations":  ["ticket", "queue", "workflow", "process", "task", "sla", "pipeline", "batch", "job"],
}

_DOMAIN_DESCRIPTIONS: dict[str, str] = {
    "sales":      "revenue sales orders purchases transactions invoices gmv",
    "inventory":  "stock inventory warehouse sku quantity reorder supply",
    "finance":    "profit expense cost margin budget forecast balance cash",
    "monitoring": "errors latency uptime cpu memory disk alerts incidents",
    "customer":   "customers users churn ltv segments cohort retention nps",
    "operations": "tickets queues workflows processes tasks sla pipelines jobs",
}

_domain_embeddings: dict[str, list[float]] = {}
_embed_lock = threading.Lock()


def _get_embedding(text: str) -> Optional[list[float]]:
    try:
        resp = requests.post(
            f"{OLLAMA_HOST}/api/embeddings",
            json={"model": OLLAMA_EMBED_MODEL, "prompt": text},
            timeout=5,
        )
        resp.raise_for_status()
        return resp.json().get("embedding")
    except Exception:
        return None


def _cosine_similarity(a: list[float], b: list[float]) -> float:
    if not _NUMPY_AVAILABLE:
        dot = sum(x * y for x, y in zip(a, b))
        mag_a = math.sqrt(sum(x * x for x in a))
        mag_b = math.sqrt(sum(x * x for x in b))
        return dot / (mag_a * mag_b + 1e-9)
    import numpy as np  # noqa
    a_arr, b_arr = np.array(a), np.array(b)
    return float(np.dot(a_arr, b_arr) / (np.linalg.norm(a_arr) * np.linalg.norm(b_arr) + 1e-9))


def _ensure_domain_embeddings():
    with _embed_lock:
        if _domain_embeddings:
            return
        for domain, desc in _DOMAIN_DESCRIPTIONS.items():
            emb = _get_embedding(desc)
            if emb:
                _domain_embeddings[domain] = emb
        if _domain_embeddings:
            log.info("[intent] Domain embeddings loaded for %d domains.", len(_domain_embeddings))
        else:
            log.warning("[intent] Embedding model unavailable — keyword fallback.")


def classify_intent(user_request: str, sql: str = "") -> str:
    _ensure_domain_embeddings()
    text = (user_request + " " + sql).strip()
    if _domain_embeddings:
        query_emb = _get_embedding(text)
        if query_emb:
            scores = {d: _cosine_similarity(query_emb, emb) for d, emb in _domain_embeddings.items()}
            best = max(scores, key=lambda d: scores[d])
            log.info("[intent] embedding scores: %s → %s", scores, best)
            return best
    text_lower = text.lower()
    kw_scores = {domain: 0 for domain in INTENT_KEYWORDS}
    for domain, keywords in INTENT_KEYWORDS.items():
        for kw in keywords:
            if kw in text_lower:
                kw_scores[domain] += 1
    best = max(kw_scores, key=lambda d: kw_scores[d])
    return best if kw_scores[best] > 0 else "sales"


def get_domain_template(domain: str) -> dict:
    return DOMAIN_TEMPLATES.get(domain, DOMAIN_TEMPLATES["sales"])


# ═══════════════════════════════════════════════════════════════════════════════
#  I-7: DASHBOARD KNOWLEDGE BASE (RAG)
# ═══════════════════════════════════════════════════════════════════════════════

@dataclass
class DashboardPattern:
    query_hash:       str
    domain:           str
    user_request:     str
    panel_type:       str
    secondary_panels: list[str]
    color_scheme:     str
    suggested_unit:   str
    score:            int
    created_at:       str


_kb_store: list[DashboardPattern] = []
_kb_lock  = threading.Lock()


def kb_store_pattern(rq: "RegisteredQuery", score: int):
    if score < DASHBOARD_SCORE_MIN:
        return
    qh = hashlib.md5(rq.user_request.lower().encode()).hexdigest()[:12]
    pattern = DashboardPattern(
        query_hash       = qh,
        domain           = rq.domain,
        user_request     = rq.user_request,
        panel_type       = rq.panel_type,
        secondary_panels = list(rq.secondary_panels),
        color_scheme     = rq.color_scheme,
        suggested_unit   = rq.suggested_unit,
        score            = score,
        created_at       = datetime.datetime.now().isoformat(timespec="seconds"),
    )
    with _kb_lock:
        _kb_store.append(pattern)
    log.info("[kb] Stored pattern for domain=%s score=%d", rq.domain, score)


def kb_find_similar(user_request: str, domain: str) -> Optional[DashboardPattern]:
    with _kb_lock:
        candidates = [p for p in _kb_store if p.domain == domain and p.score >= DASHBOARD_SCORE_MIN]
    if not candidates:
        return None
    _ensure_domain_embeddings()
    if _domain_embeddings:
        query_emb = _get_embedding(user_request)
        if query_emb:
            best_p, best_score = None, -1.0
            for p in candidates:
                p_emb = _get_embedding(p.user_request)
                if p_emb:
                    sim = _cosine_similarity(query_emb, p_emb)
                    if sim > best_score:
                        best_score, best_p = sim, p
            if best_p and best_score > 0.75:
                log.info("[kb] RAG match (sim=%.3f): %s", best_score, best_p.user_request[:60])
                return best_p
    req_words = set(user_request.lower().split())
    best_p, best_overlap = None, 0
    for p in candidates:
        overlap = len(req_words & set(p.user_request.lower().split()))
        if overlap > best_overlap:
            best_overlap, best_p = overlap, p
    if best_p and best_overlap >= 2:
        log.info("[kb] RAG keyword match (overlap=%d): %s", best_overlap, best_p.user_request[:60])
        return best_p
    return None


# ═══════════════════════════════════════════════════════════════════════════════
#  BLUEPRINT (retained as heuristic fallback + data structure)
# ═══════════════════════════════════════════════════════════════════════════════

@dataclass
class DashboardBlueprint:
    domain:           str
    title:            str
    primary_panel:    str
    secondary_panels: list[str]
    color_scheme:     str
    suggested_unit:   str
    has_timeseries:   bool
    has_categories:   bool
    kpi_count:        int


def build_blueprint(
    domain: str,
    col_names: list[str],
    has_datetime: bool,
    num_cols: list[str],
    str_cols: list[str],
    user_request: str,
    kb_pattern: Optional[DashboardPattern] = None,
    force_panel_type: Optional[str] = None,
) -> DashboardBlueprint:
    """
    RETAINED for use as internal fallback inside DashboardPlannerAgent.
    In v4.2, plan_dashboard() is the primary entry point.
    """
    tmpl = get_domain_template(domain)
    if force_panel_type:
        primary = force_panel_type
    elif kb_pattern:
        return DashboardBlueprint(
            domain=domain,
            title=f"{tmpl['title_prefix']} — {_short_title(user_request)}",
            primary_panel=kb_pattern.panel_type,
            secondary_panels=kb_pattern.secondary_panels,
            color_scheme=kb_pattern.color_scheme,
            suggested_unit=kb_pattern.suggested_unit,
            has_timeseries=has_datetime, has_categories=bool(str_cols),
            kpi_count=len(num_cols),
        )
    else:
        primary = tmpl["primary_panel"]
        if has_datetime and num_cols:
            primary = "timeseries"
        elif not str_cols and num_cols:
            primary = "stat"
        elif len(str_cols) == 1 and num_cols:
            primary = "barchart"
    return DashboardBlueprint(
        domain=domain,
        title=f"{tmpl['title_prefix']} — {_short_title(user_request)}",
        primary_panel=primary,
        secondary_panels=tmpl["secondary_panels"][:2],
        color_scheme=tmpl["color_scheme"],
        suggested_unit=tmpl["suggested_unit"],
        has_timeseries=has_datetime, has_categories=bool(str_cols),
        kpi_count=len(num_cols),
    )


def _short_title(text: str) -> str:
    words = text.split()[:6]
    return " ".join(w.capitalize() for w in words)


# ═══════════════════════════════════════════════════════════════════════════════
#  v4.2: DASHBOARD PLANNING AGENT
#  Primary layout decision maker. Replaces direct build_blueprint() calls in
#  register_query() and push_to_grafana(). build_blueprint() is retained as
#  the heuristic fallback INSIDE DashboardPlannerAgent._heuristic_fallback().
# ═══════════════════════════════════════════════════════════════════════════════

VALID_PANEL_TYPES_PLANNER = {
    "timeseries", "barchart", "piechart", "bargauge",
    "stat", "table", "gauge", "histogram", "heatmap",
}
VALID_SECONDARY_TYPES_PLANNER = {"stat", "table", "barchart", "bargauge", "timeseries"}
VALID_COLOR_SCHEMES_PLANNER   = {"green", "red", "blue", "orange", "purple", "auto"}
VALID_UNITS_PLANNER = {
    "currencyUSD", "currencyEUR", "percent", "percentunit",
    "ms", "s", "bytes", "short", "none", "reqps", "ops",
}

_PLANNER_SYSTEM_PROMPT = """\
You are an expert Grafana dashboard architect and business intelligence analyst.

Your job: given a business question, its SQL query, the result columns/sample data,
and the detected domain, DESIGN the optimal Grafana dashboard layout.

You must reason about:
  1. What story the user actually wants to tell (not just what the SQL returns)
  2. Whether the data is time-series, categorical, KPI, or mixed
  3. Which Grafana panel type best communicates the answer
  4. What secondary panels add context without cluttering
  5. The right color palette and unit for the domain
  6. Whether a top-N filter or trend view is more useful

CRITICAL RULE — same SQL, different intent = different dashboard:
  "Show me sales by region"        → barchart  (compare regions)
  "Show me how total revenue grew" → timeseries (show trend)
  "Give me a quick sales KPI"      → stat       (single number)
  "Show full breakdown"            → table      (all detail)
You MUST select the panel type that matches the QUESTION, not just the data shape.

STRICT OUTPUT RULES:
  - Respond ONLY with a single valid JSON object. No markdown, no prose.
  - primary_panel_type must be one of: timeseries, barchart, piechart, bargauge, stat, table, gauge, histogram, heatmap
  - secondary_panels items must be from: stat, table, barchart, bargauge, timeseries
  - color_scheme must be one of: green, red, blue, orange, purple, auto
  - suggested_unit must be one of: currencyUSD, currencyEUR, percent, percentunit, ms, s, bytes, short, none, reqps, ops
  - confidence is a float 0.0-1.0

JSON schema (respond with EXACTLY this structure, no extra keys):
{
  "primary_panel_type":   string,
  "primary_panel_title":  string,
  "primary_panel_reason": string,
  "secondary_panels":     [string, string],
  "secondary_titles":     [string, string],
  "color_scheme":         string,
  "suggested_unit":       string,
  "thresholds_strategy":  "auto" | "traffic_light" | "none",
  "show_timeseries":      bool,
  "highlight_top_n":      int,
  "narrative_focus":      string,
  "confidence":           float
}"""

_PLANNER_USER_PROMPT = """\
BUSINESS QUESTION:
{user_request}

DETECTED DOMAIN: {domain}

SQL QUERY:
{sql}

RESULT COLUMNS (with inferred types):
{col_info}

SAMPLE DATA (top 6 rows):
{sample_rows}

NUMERIC COLUMNS:  {num_cols}
LABEL COLUMNS:    {str_cols}
DATETIME COLUMNS: {datetime_cols}
HAS DATETIME:     {has_datetime}

ADDITIONAL CONTEXT:
{context_notes}

Design the optimal dashboard layout. Respond ONLY with the JSON object.
JSON:"""


@dataclass
class DashboardPlan:
    """
    Structured output from DashboardPlannerAgent.

    All fields have safe defaults so partial LLM output can still be used.
    confidence < 0.4 signals that the heuristic fallback was used.
    """
    primary_panel_type:   str   = "barchart"
    primary_panel_title:  str   = "Metric Overview"
    primary_panel_reason: str   = ""
    secondary_panels:     list  = field(default_factory=lambda: ["stat", "table"])
    secondary_titles:     list  = field(default_factory=list)
    color_scheme:         str   = "auto"
    suggested_unit:       str   = "short"
    thresholds_strategy:  str   = "auto"
    show_timeseries:      bool  = False
    highlight_top_n:      int   = 0
    narrative_focus:      str   = ""
    confidence:           float = 1.0


class DashboardPlannerAgent:
    """
    LLM-powered dashboard planning agent.

    Reasons about business intent, data shape, and domain context to select
    the optimal Grafana panel layout — so the same SQL with a different
    business question produces a different, contextually correct dashboard.

    Falls back to rule-based heuristics (build_blueprint logic) when the LLM
    is unavailable or returns invalid JSON, ensuring zero regression.
    """

    def __init__(self, llm_client):
        self._llm = llm_client

    # ── Public API ─────────────────────────────────────────────────────────────

    def plan(
        self,
        user_request:  str,
        sql:           str,
        col_names:     list,
        rows:          list,
        domain:        str,
        num_cols:      list,
        str_cols:      list,
        datetime_cols: list,
        has_datetime:  bool,
        kb_pattern:    Optional[DashboardPattern] = None,
    ) -> DashboardPlan:
        """
        Invoke the LLM to produce a DashboardPlan.
        Returns a validated DashboardPlan (heuristic fallback on any failure).
        """
        try:
            prompt = self._build_prompt(
                user_request, sql, col_names, rows, domain,
                num_cols, str_cols, datetime_cols, has_datetime, kb_pattern,
            )
            raw = self._llm.invoke([
                SystemMessage(content=_PLANNER_SYSTEM_PROMPT),
                HumanMessage(content=prompt),
            ]).content.strip()

            plan = self._parse_and_validate(raw)
            log.info(
                "[planner] primary=%s secondary=%s unit=%s color=%s "
                "top_n=%d confidence=%.2f narrative=%.80s reason=%.60s",
                plan.primary_panel_type, plan.secondary_panels,
                plan.suggested_unit, plan.color_scheme,
                plan.highlight_top_n, plan.confidence,
                plan.narrative_focus, plan.primary_panel_reason,
            )
            return plan

        except Exception as exc:
            log.warning("[planner] LLM call failed (%s) — heuristic fallback.", exc)
            return self._heuristic_fallback(domain, has_datetime, num_cols, str_cols)

    # ── Private ────────────────────────────────────────────────────────────────

    def _build_prompt(
        self, user_request, sql, col_names, rows, domain,
        num_cols, str_cols, datetime_cols, has_datetime, kb_pattern,
    ) -> str:
        sample = rows[0] if rows else []
        col_info_lines = []
        for i, col in enumerate(col_names):
            val = sample[i] if i < len(sample) else None
            col_info_lines.append(f"  {col}: {_planner_infer_type(col, val)}")

        sample_lines = []
        for row in rows[:6]:
            parts = [f"{col_names[j]}={_planner_fmt_val(row[j])}"
                     for j in range(min(len(col_names), 6))]
            sample_lines.append("  " + ", ".join(parts))

        context_parts = []
        if kb_pattern:
            context_parts.append(
                f"Similar past query used primary_panel='{kb_pattern.panel_type}' "
                f"(score={kb_pattern.score}/100). Reuse if business context matches."
            )
        if len(rows) == 1:
            context_parts.append("Single-row result — KPI/stat panel is likely best.")
        if len(rows) > 200:
            context_parts.append(
                f"Large result ({len(rows)} rows) — consider top-N or aggregation panel.")
        if len(num_cols) > 3:
            context_parts.append(
                f"Many numeric columns ({len(num_cols)}) — focus on 1-2 most business-relevant.")

        return _PLANNER_USER_PROMPT.format(
            user_request  = user_request,
            domain        = domain,
            sql           = sql[:800],
            col_info      = "\n".join(col_info_lines) or "  (none)",
            sample_rows   = "\n".join(sample_lines) or "  (no data)",
            num_cols      = ", ".join(num_cols) or "(none)",
            str_cols      = ", ".join(str_cols) or "(none)",
            datetime_cols = ", ".join(datetime_cols) or "(none)",
            has_datetime  = has_datetime,
            context_notes = "\n".join(context_parts) or "None.",
        )

    def _parse_and_validate(self, raw: str) -> DashboardPlan:
        raw = re.sub(r"```(?:json)?", "", raw, flags=re.IGNORECASE).replace("```", "").strip()
        m   = re.search(r"\{.*\}", raw, re.DOTALL)
        if not m:
            raise ValueError("No JSON object found in planner response.")
        data = json.loads(m.group(0))

        # primary_panel_type
        ptype = data.get("primary_panel_type", "barchart")
        if ptype not in VALID_PANEL_TYPES_PLANNER:
            log.warning("[planner] Unknown primary type '%s' → barchart", ptype)
            ptype = "barchart"

        # secondary_panels (exactly 2)
        raw_sec  = data.get("secondary_panels", ["stat", "table"])
        secondary = [s for s in (raw_sec if isinstance(raw_sec, list) else [])
                     if s in VALID_SECONDARY_TYPES_PLANNER][:2]
        while len(secondary) < 2:
            secondary.append("table")

        # secondary_titles
        sec_titles = data.get("secondary_titles", [])
        if not isinstance(sec_titles, list):
            sec_titles = []
        while len(sec_titles) < 2:
            sec_titles.append("")

        # color_scheme
        color = data.get("color_scheme", "auto")
        if color not in VALID_COLOR_SCHEMES_PLANNER:
            color = "auto"

        # suggested_unit
        unit = data.get("suggested_unit", "short")
        if unit not in VALID_UNITS_PLANNER:
            unit = "short"

        # thresholds_strategy
        thresh = data.get("thresholds_strategy", "auto")
        if thresh not in ("auto", "traffic_light", "none"):
            thresh = "auto"

        # confidence
        try:
            confidence = float(data.get("confidence", 1.0))
            confidence = max(0.0, min(1.0, confidence))
        except (TypeError, ValueError):
            confidence = 0.5

        return DashboardPlan(
            primary_panel_type   = ptype,
            primary_panel_title  = str(data.get("primary_panel_title", "Metric Overview"))[:80],
            primary_panel_reason = str(data.get("primary_panel_reason", ""))[:200],
            secondary_panels     = secondary,
            secondary_titles     = sec_titles[:2],
            color_scheme         = color,
            suggested_unit       = unit,
            thresholds_strategy  = thresh,
            show_timeseries      = bool(data.get("show_timeseries", False)),
            highlight_top_n      = max(0, int(data.get("highlight_top_n", 0) or 0)),
            narrative_focus      = str(data.get("narrative_focus", ""))[:200],
            confidence           = confidence,
        )

    @staticmethod
    def _heuristic_fallback(
        domain: str, has_datetime: bool, num_cols: list, str_cols: list,
    ) -> DashboardPlan:
        """Mirror build_blueprint() logic — guarantees zero regression."""
        _DEFAULTS = {
            "sales":      ("timeseries", "green",  "currencyUSD"),
            "finance":    ("timeseries", "green",  "currencyUSD"),
            "inventory":  ("bargauge",   "orange", "short"),
            "monitoring": ("timeseries", "red",    "ms"),
            "customer":   ("barchart",   "blue",   "short"),
            "operations": ("table",      "purple", "short"),
        }
        ptype, color, unit = _DEFAULTS.get(domain, ("barchart", "auto", "short"))
        if has_datetime and num_cols:
            ptype = "timeseries"
        elif not str_cols and num_cols:
            ptype = "stat"
        elif len(str_cols) == 1 and num_cols:
            ptype = "barchart"
        return DashboardPlan(
            primary_panel_type = ptype,
            secondary_panels   = ["stat", "table"],
            color_scheme       = color,
            suggested_unit     = unit,
            confidence         = 0.0,   # signals: heuristic was used
        )


# Singleton agent — one instance per process
_dashboard_planner_agent: Optional[DashboardPlannerAgent] = None
_planner_lock = threading.Lock()


def _get_planner_agent() -> DashboardPlannerAgent:
    global _dashboard_planner_agent
    with _planner_lock:
        if _dashboard_planner_agent is None:
            _dashboard_planner_agent = DashboardPlannerAgent(chat_llm)
    return _dashboard_planner_agent


def plan_dashboard(
    user_request:     str,
    sql:              str,
    col_names:        list,
    rows:             list,
    domain:           str,
    num_cols:         list,
    str_cols:         list,
    datetime_cols:    list,
    has_datetime:     bool,
    kb_pattern:       Optional[DashboardPattern] = None,
    force_panel_type: Optional[str]              = None,
) -> tuple[DashboardBlueprint, DashboardPlan]:
    """
    v4.2 primary entry point — replaces build_blueprint() in register_query()
    and push_to_grafana().

    Returns (DashboardBlueprint, DashboardPlan):
      - DashboardBlueprint is backward-compatible with all existing callers.
      - DashboardPlan exposes planning metadata (narrative_focus, highlight_top_n,
        confidence, primary_panel_reason, etc.) for logging and API responses.

    force_panel_type: honoured from the score-feedback retry loop (I-5 / DPA-2).
    """
    agent = _get_planner_agent()
    plan  = agent.plan(
        user_request  = user_request,
        sql           = sql,
        col_names     = col_names,
        rows          = rows,
        domain        = domain,
        num_cols      = num_cols,
        str_cols      = str_cols,
        datetime_cols = datetime_cols,
        has_datetime  = has_datetime,
        kb_pattern    = kb_pattern,
    )

    # DPA-2: Honour force_panel_type from retry loop
    if force_panel_type:
        log.info("[planner] force_panel_type=%s overrides plan.primary=%s",
                 force_panel_type, plan.primary_panel_type)
        plan.primary_panel_type = force_panel_type

    # Convert DashboardPlan → DashboardBlueprint (backward-compatible)
    tmpl  = get_domain_template(domain)
    title = plan.primary_panel_title or f"{tmpl['title_prefix']} — {_short_title(user_request)}"

    blueprint = DashboardBlueprint(
        domain           = domain,
        title            = title,
        primary_panel    = plan.primary_panel_type,
        secondary_panels = plan.secondary_panels,
        color_scheme     = plan.color_scheme,
        suggested_unit   = plan.suggested_unit,
        has_timeseries   = has_datetime or plan.show_timeseries,
        has_categories   = bool(str_cols),
        kpi_count        = len(num_cols),
    )
    return blueprint, plan


def _planner_infer_type(col_name: str, sample_val) -> str:
    if sample_val is None:
        return "unknown"
    try:
        float(sample_val)
        return "numeric"
    except (TypeError, ValueError):
        pass
    if isinstance(sample_val, (datetime.date, datetime.datetime)):
        return "datetime"
    if isinstance(sample_val, str) and len(sample_val) <= 64:
        return "text/category"
    return "text"


def _planner_fmt_val(v) -> str:
    if v is None:
        return "null"
    s = str(v)
    return (s[:30] + "…") if len(s) > 30 else s


# ═══════════════════════════════════════════════════════════════════════════════
#  DATETIME DETECTION
# ═══════════════════════════════════════════════════════════════════════════════

def detect_datetime_columns(col_names: list[str], rows: list[tuple]) -> list[str]:
    datetime_cols = []
    if not rows:
        return datetime_cols
    sample_row = rows[0]
    for i, col in enumerate(col_names):
        if DATETIME_COL_PATTERNS.search(col):
            datetime_cols.append(col)
            continue
        val = sample_row[i]
        if isinstance(val, (datetime.date, datetime.datetime)):
            datetime_cols.append(col)
            continue
        if isinstance(val, str):
            for fmt in ("%Y-%m-%d", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S"):
                try:
                    datetime.datetime.strptime(val[:19], fmt)
                    datetime_cols.append(col)
                    break
                except ValueError:
                    pass
    return list(dict.fromkeys(datetime_cols))


# ═══════════════════════════════════════════════════════════════════════════════
#  STATISTICAL THRESHOLDS (BF-3 fix retained)
# ═══════════════════════════════════════════════════════════════════════════════

def compute_statistical_thresholds(
    col_name: str, rows: list[tuple], col_idx: int, domain: str
) -> Optional[dict]:
    values = []
    for row in rows:
        try:
            if row[col_idx] is not None:
                values.append(float(row[col_idx]))
        except (TypeError, ValueError):
            pass
    if len(values) < 5:
        return _static_thresholds(col_name, domain)
    values.sort()
    n   = len(values)
    p50 = values[min(int(n * 0.50), n - 1)]
    p90 = values[min(int(n * 0.90), n - 1)]
    col_lower = col_name.lower()
    if re.search(r"(error|fail|latency|cpu|memory|disk)", col_lower) or domain == "monitoring":
        return {"mode": "absolute", "steps": [
            {"color": "green",  "value": None},
            {"color": "yellow", "value": round(p50, 2)},
            {"color": "red",    "value": round(p90, 2)},
        ]}
    if re.search(r"(stock|inventory|quantity)", col_lower) or domain == "inventory":
        return {"mode": "absolute", "steps": [
            {"color": "red",    "value": None},
            {"color": "yellow", "value": round(p50, 2)},
            {"color": "green",  "value": round(p90, 2)},
        ]}
    return {"mode": "absolute", "steps": [
        {"color": "blue",   "value": None},
        {"color": "green",  "value": round(p50, 2)},
        {"color": "orange", "value": round(p90, 2)},
    ]}


def _static_thresholds(col_name: str, domain: str) -> Optional[dict]:
    col = col_name.lower()
    if re.search(r"(cpu|memory|disk|error|fail)", col) or domain == "monitoring":
        return {"mode": "absolute", "steps": [
            {"color": "green",  "value": None},
            {"color": "yellow", "value": 70},
            {"color": "red",    "value": 90},
        ]}
    if re.search(r"(stock|inventory|quantity)", col) or domain == "inventory":
        return {"mode": "absolute", "steps": [
            {"color": "red",    "value": None},
            {"color": "yellow", "value": 10},
            {"color": "green",  "value": 50},
        ]}
    return None


# ═══════════════════════════════════════════════════════════════════════════════
#  DASHBOARD SCORING ENGINE
# ═══════════════════════════════════════════════════════════════════════════════

def score_dashboard(panels: list[dict], rq: "RegisteredQuery") -> dict:
    issues, breakdown = [], {}
    relevance = 30
    if rq.has_datetime and not any(p.get("type") == "timeseries" for p in panels):
        relevance -= 15; issues.append("Datetime column but no timeseries panel.")
    if not rq.num_cols:
        relevance -= 20; issues.append("No numeric columns.")
    if rq.panel_type == "table" and len(rq.num_cols) > 1:
        relevance -= 5; issues.append("Multiple numeric cols but plain table.")
    breakdown["panel_relevance"] = max(0, relevance)

    layout_score = 25
    grid_cells: set[tuple] = set()
    for p in panels:
        gp = p.get("gridPos", {})
        x, y, w, h = gp.get("x", 0), gp.get("y", 0), gp.get("w", 1), gp.get("h", 1)
        overlap = False
        for dx in range(w):
            for dy in range(h):
                cell = (x + dx, y + dy)
                if cell in grid_cells and not overlap:
                    layout_score -= 10; overlap = True
                    issues.append(f"Panel '{p.get('title','')}' overlaps at ({x+dx},{y+dy}).")
                grid_cells.add(cell)
        if w > 24 or h < 2:
            layout_score -= 3; issues.append(f"Panel '{p.get('title','')}' unusual dims.")
    breakdown["layout_quality"] = max(0, layout_score)

    field_score = 25
    for p in panels:
        fc = p.get("fieldConfig", {}).get("defaults", {})
        if not fc.get("unit") or fc.get("unit") == "none":
            field_score -= 3; issues.append(f"Panel '{p.get('title','')}' missing unit.")
        if not fc.get("thresholds"):
            field_score -= 2
    breakdown["field_mappings"] = max(0, field_score)

    label_score = 20
    for p in panels:
        title = p.get("title", "")
        if not title or title.lower() in ("panel", "chart", "graph", "untitled"):
            label_score -= 4; issues.append(f"Generic/missing title: '{title}'.")
        if not p.get("description"):
            label_score -= 1
    breakdown["label_quality"] = max(0, label_score)

    return {"total": sum(breakdown.values()), "breakdown": breakdown, "issues": issues}


# ═══════════════════════════════════════════════════════════════════════════════
#  DASHBOARD JSON VALIDATION
# ═══════════════════════════════════════════════════════════════════════════════

VALID_PANEL_TYPES = {
    "timeseries", "barchart", "piechart", "bargauge",
    "stat", "table", "text", "gauge", "histogram",
    "candlestick", "heatmap", "logs", "news",
}

VALID_TRANSFORM_IDS = {
    "reduce", "organize", "merge", "sortBy", "limit", "calculateField",
    "filterByName", "filterByValue", "groupBy", "concatenate",
    "seriesToRows", "labelsToFields", "rowsToFields",
}


def validate_dashboard_json(payload: dict) -> list[str]:
    errors: list[str] = []
    dash = payload.get("dashboard", {})
    if not dash.get("title"):
        errors.append("Dashboard missing 'title'.")
    if not isinstance(dash.get("panels"), list):
        errors.append("Dashboard 'panels' must be a list.")
        return errors
    grid_cells: set[tuple] = set()
    for idx, panel in enumerate(dash["panels"]):
        tag   = f"Panel[{idx}] '{panel.get('title','<no title>')}'"
        ptype = panel.get("type", "")
        if ptype not in VALID_PANEL_TYPES:
            errors.append(f"{tag}: unknown panel type '{ptype}'.")
        gp = panel.get("gridPos", {})
        x, y, w, h = gp.get("x", 0), gp.get("y", 0), gp.get("w", 1), gp.get("h", 1)
        for dx in range(w):
            for dy in range(h):
                cell = (x + dx, y + dy)
                if cell in grid_cells:
                    errors.append(f"{tag}: grid overlap at ({x+dx},{y+dy}).")
                grid_cells.add(cell)
        ds = panel.get("datasource")
        if ds and isinstance(ds, dict) and not ds.get("uid") and not ds.get("type"):
            errors.append(f"{tag}: datasource missing 'uid' and 'type'.")
        for t in panel.get("transformations", []):
            tid = t.get("id", "")
            if tid not in VALID_TRANSFORM_IDS:
                errors.append(f"{tag}: unknown transformation '{tid}'.")
        fc_defaults = panel.get("fieldConfig", {}).get("defaults", {})
        if "thresholds" in fc_defaults:
            if not isinstance(fc_defaults["thresholds"].get("steps"), list):
                errors.append(f"{tag}: fieldConfig.thresholds.steps must be a list.")
    return errors


# ═══════════════════════════════════════════════════════════════════════════════
#  I-1: BATCHED LLM CALL — panel descriptions + insights + title
# ═══════════════════════════════════════════════════════════════════════════════

def generate_enrichment_batch(
    user_request: str,
    panel_title: str,
    panel_type: str,
    primary_col: str,
    domain: str,
    col_names: list[str],
    rows: list[tuple],
    secondary_panels: list[str],
) -> dict:
    sample_lines = []
    for row in rows[:8]:
        parts = [f"{col_names[i]}={row[i]}" for i in range(min(len(col_names), 5))]
        sample_lines.append(", ".join(parts))
    sample_text = "\n".join(sample_lines) if sample_lines else "No data available."
    sec_count = len(secondary_panels)
    prompt = f"""You are a senior business data analyst and Grafana dashboard designer.

Domain: {domain}
Business question: {user_request}
Primary panel type: {panel_type}
Primary metric: {primary_col}
Secondary panels: {secondary_panels}

Sample data (top 8 rows):
{sample_text}

Respond ONLY with a single valid JSON object (no markdown, no extra text):
{{
  "title": "<refined 6-8 word dashboard title>",
  "main_description": "<one clear sentence describing the {panel_type} panel showing {primary_col}>",
  "secondary_descriptions": [<{sec_count} one-sentence descriptions, one per secondary panel>],
  "insights": "<3-5 bullet points starting with •, each max 20 words, business-focused>"
}}

JSON:"""
    try:
        raw = chat_llm.invoke([HumanMessage(content=prompt)]).content.strip()
        raw = re.sub(r"```(?:json)?", "", raw, flags=re.IGNORECASE).replace("```", "").strip()
        m = re.search(r"\{.*\}", raw, re.DOTALL)
        if m:
            data = json.loads(m.group(0))
            data.setdefault("title", panel_title)
            data.setdefault("main_description", f"Shows {primary_col} for {domain} analysis.")
            data.setdefault("secondary_descriptions", [
                f"Summary view of {primary_col} for {domain}." for _ in secondary_panels
            ])
            data.setdefault("insights", f"• Key metrics for {domain} shown above.")
            while len(data["secondary_descriptions"]) < sec_count:
                data["secondary_descriptions"].append(f"Additional {domain} metric view.")
            return data
    except Exception as exc:
        log.warning("[enrich] Batched LLM call failed: %s", exc)
    return {
        "title":                  panel_title,
        "main_description":       f"Shows {primary_col.replace('_', ' ')} for {domain} analysis.",
        "secondary_descriptions": [f"Summary view of {primary_col} for {domain}." for _ in secondary_panels],
        "insights":               f"• Data loaded for {domain}. Review panels above for key trends.",
    }


# ═══════════════════════════════════════════════════════════════════════════════
#  SMART VARIABLES
# ═══════════════════════════════════════════════════════════════════════════════

_SEMANTIC_FILTER_PATTERNS: dict[str, re.Pattern] = {
    "Region":           re.compile(r"\b(region|country|state|city|location|geography|territory)\b", re.I),
    "Category":         re.compile(r"\b(category|type|class|segment|group|kind|tier)\b", re.I),
    "Department":       re.compile(r"\b(department|dept|team|division|unit|branch)\b", re.I),
    "Customer Segment": re.compile(r"\b(segment|customer_type|plan|subscription|level)\b", re.I),
    "Status":           re.compile(r"\b(status|state|stage|phase|condition)\b", re.I),
    "Product":          re.compile(r"\b(product|sku|item|goods|service|offering)\b", re.I),
}


def build_smart_variables(label_names: list[str], metric_names: list[str], ds_uid: str) -> list[dict]:
    variables = []
    seen = set()
    for raw_label in label_names[:5]:
        if raw_label in seen:
            continue
        seen.add(raw_label)
        display_label = raw_label.replace("_", " ").title()
        for sem_name, pat in _SEMANTIC_FILTER_PATTERNS.items():
            if pat.search(raw_label):
                display_label = sem_name
                break
        metric_name = metric_names[0] if metric_names else ""
        variables.append({
            "name": raw_label, "type": "query", "label": display_label,
            "query": f"label_values({metric_name}, {raw_label})",
            "datasource": {"type": "prometheus", "uid": ds_uid},
            "includeAll": True, "multi": True,
            "current": {"text": "All", "value": "$__all"},
            "refresh": 2, "sort": 1, "hide": 0,
            "definition": f"label_values({metric_name}, {raw_label})",
        })
    return variables


# ═══════════════════════════════════════════════════════════════════════════════
#  I-4: DASHBOARD VERSIONING
# ═══════════════════════════════════════════════════════════════════════════════

@dataclass
class DashboardVersion:
    version:    int
    created_at: str
    payload:    dict
    score:      int
    issues:     list[str]


_version_store: dict[str, list[DashboardVersion]] = {}
_version_lock  = threading.Lock()


def store_dashboard_version(uid: str, payload: dict, score: int, issues: list[str]) -> int:
    with _version_lock:
        history = _version_store.setdefault(uid, [])
        version_num = len(history) + 1
        history.append(DashboardVersion(
            version    = version_num,
            created_at = datetime.datetime.now().isoformat(timespec="seconds"),
            payload    = payload, score=score, issues=issues,
        ))
        return version_num


def get_dashboard_history(uid: str) -> list[dict]:
    with _version_lock:
        return [
            {"version": v.version, "created_at": v.created_at,
             "score": v.score, "issues": v.issues}
            for v in _version_store.get(uid, [])
        ]


def rollback_dashboard(uid: str, version: int) -> Optional[dict]:
    with _version_lock:
        for v in _version_store.get(uid, []):
            if v.version == version:
                return v.payload
    return None


# ═══════════════════════════════════════════════════════════════════════════════
#  I-8: DASHBOARD QUALITY REPORT PANEL
# ═══════════════════════════════════════════════════════════════════════════════

def build_quality_report_panel(score_result: dict, version: int, y_offset: int) -> dict:
    total     = score_result["total"]
    breakdown = score_result["breakdown"]
    issues    = score_result["issues"]
    if total >= 90:   grade = "🟢 Excellent"
    elif total >= 80: grade = "🟡 Good"
    elif total >= 65: grade = "🟠 Fair"
    else:             grade = "🔴 Needs Work"
    strengths = [k.replace("_", " ").title() for k, v in breakdown.items() if v >= (25 * 0.8)]
    lines = [
        f"## Dashboard Quality Report — v{version}", "",
        f"**Overall Score: {total}/100** — {grade}", "",
        "| Dimension | Score |", "| --- | --- |",
    ]
    for dim, val in breakdown.items():
        lines.append(f"| {dim.replace('_',' ').title()} | {val} |")
    if strengths:
        lines += ["", "**Strengths:**"]
        for s in strengths:
            lines.append(f"✓ {s}")
    if issues:
        lines += ["", "**Issues:**"]
        for issue in issues[:6]:
            lines.append(f"⚠ {issue}")
    else:
        lines += ["", "✓ No layout issues detected."]
    return {
        "type": "text", "title": "Dashboard Quality Report",
        "description": "Auto-generated quality assessment for this dashboard.",
        "gridPos": {"x": 0, "y": y_offset, "w": 24, "h": 8},
        "options": {"mode": "markdown", "content": "\n".join(lines)},
    }



@dataclass
class RegisteredQuery:
    query_id:         str
    user_request:     str
    sql:              str
    title:            str
    description:      str
    panel_type:       str
    metric_prefix:    str
    num_cols:         list
    str_cols:         list
    label_names:      list
    metric_names:     list
    grafana_uid:      str
    grafana_url:      str   = ""
    row_count:        int   = 0
    created_at:       str   = ""
    plan:             str   = ""
    cost_estimate:    float = 0.0
    domain:           str   = "general"
    suggested_unit:   str   = "none"
    color_scheme:     str   = "auto"
    secondary_panels: list  = field(default_factory=list)
    top_n:            int   = 0
    tags:             list  = field(default_factory=list)
    has_datetime:     bool  = False
    datetime_cols:    list  = field(default_factory=list)
    dashboard_score:  int   = 0
    current_version:  int   = 1
    # v4.2: planning metadata stored on the query for inspection
    plan_narrative:   str   = ""
    plan_confidence:  float = 1.0


_registered:      dict[str, RegisteredQuery] = {}
_registered_lock  = threading.Lock()


# ═══════════════════════════════════════════════════════════════════════════════
#  SCRAPE LOOP
# ═══════════════════════════════════════════════════════════════════════════════

def scrape_query(rq: RegisteredQuery):
    up_g = _get_up_gauge()
    conn = None
    try:
        conn = _pg_connect()
        col_names, rows = execute_query_psycopg2(conn, rq.sql)
        if rows:
            push_metrics(rq, col_names, rows)
        up_g.labels(query_id=rq.query_id, title=rq.title[:64]).set(1)
        log.info("[scrape] %s rows=%d", rq.title, len(rows))
    except Exception as exc:
        up_g.labels(query_id=rq.query_id, title=rq.title[:64]).set(0)
        log.error("[scrape] %s FAIL: %s", rq.title, exc)
    finally:
        if conn:
            conn.close()


def scrape_loop():
    while True:
        with _registered_lock:
            queries = list(_registered.values())
        for rq in queries:
            scrape_query(rq)
        time.sleep(SCRAPE_INTERVAL)


# ═══════════════════════════════════════════════════════════════════════════════
#  GRAFANA HELPERS
# ═══════════════════════════════════════════════════════════════════════════════

def _grafana_auth():
    return None if GRAFANA_API_KEY else (GRAFANA_USER, GRAFANA_PASS)


def _grafana_headers() -> dict:
    h = {"Content-Type": "application/json"}
    if GRAFANA_API_KEY:
        h["Authorization"] = f"Bearer {GRAFANA_API_KEY}"
    return h


def get_prometheus_ds_uid() -> str:
    try:
        r = requests.get(f"{GRAFANA_URL}/api/datasources",
                         auth=_grafana_auth(), headers=_grafana_headers(), timeout=5)
        r.raise_for_status()
        for ds in r.json():
            if ds.get("type") == "prometheus" or \
               PROMETHEUS_DS_NAME.lower() in ds.get("name", "").lower():
                return ds.get("uid") or ds.get("name")
    except Exception as exc:
        log.warning("[Grafana] datasources: %s", exc)
    return PROMETHEUS_DS_NAME


_UNIT_MAP = {
    "currencyUSD": r"(revenue|sales|amount|price|cost|profit|income|spend|gmv|arr|mrr)",
    "percent":     r"(percent|pct|rate|ratio|share|growth|margin)",
    "ms":          r"(latency|duration|response_time|elapsed|wait|p99|p95|p50)",
    "bytes":       r"(bytes|size|storage|memory|disk)",
    "short":       r"(count|total|sum|num|qty|quantity)",
}


def _detect_unit(col_name: str, suggested: str = "none") -> str:
    if suggested and suggested != "none":
        return suggested
    col = col_name.lower()
    for unit, pattern in _UNIT_MAP.items():
        if re.search(pattern, col):
            return unit
    return "short"


_DOMAIN_PALETTE = {
    "sales": "green", "finance": "green", "inventory": "orange",
    "customers": "blue", "customer": "blue", "monitoring": "red",
    "system": "purple", "errors": "red", "operations": "purple",
    "general": "auto",
}

_PALETTE_FIXED = {
    "green":  ["#00d4ff", "#00ff9d", "#73bf69", "#37872d"],
    "red":    ["#ff5f52", "#ff7383", "#fa6400", "#e02f44"],
    "blue":   ["#5794f2", "#73a8f7", "#a0c4ff", "#2a65c0"],
    "orange": ["#ff9830", "#ffb357", "#ffd07e", "#e07400"],
    "purple": ["#b877d9", "#c89aef", "#9561c2", "#6e3fcb"],
    "auto":   [],
}


def _color_config(rq: RegisteredQuery) -> dict:
    scheme = rq.color_scheme if rq.color_scheme != "auto" else _DOMAIN_PALETTE.get(rq.domain, "auto")
    if scheme == "auto" or not _PALETTE_FIXED.get(scheme):
        return {"mode": "palette-classic"}
    return {"mode": "fixed", "fixedColor": _PALETTE_FIXED[scheme][0]}


def _generate_tags(user_request: str, domain: str) -> list[str]:
    tags = ["nl-agent", "postgres", "auto-generated", domain]
    keywords = re.findall(
        r"\b(sales|order|customer|product|revenue|inventory|user|error|cpu|memory)\b",
        user_request.lower(),
    )
    tags.extend(list(dict.fromkeys(keywords)))
    return tags[:8]


def _transforms_category(rq: RegisteredQuery) -> list[dict]:
    if not rq.label_names:
        return [{"id": "reduce", "options": {"reducers": ["lastNotNull"], "includeTimeField": False}}]
    primary_label = rq.label_names[0]
    transforms = [
        {"id": "reduce", "options": {"reducers": ["lastNotNull"], "includeTimeField": False, "mode": "seriesToRows"}},
        {"id": "organize", "options": {
            "renameByName": {"Field": primary_label, "Last *": "value"},
            "indexByName":  {primary_label: 0, "value": 1},
            "excludeByName": {},
        }},
    ]
    if rq.top_n > 0:
        transforms += [
            {"id": "sortBy",  "options": {"fields": [{"displayName": "value", "desc": True}]}},
            {"id": "limit",   "options": {"limitField": rq.top_n}},
        ]
    return transforms


def _transforms_raw_table(rq: RegisteredQuery) -> list[dict]:
    if not rq.label_names:
        return [{"id": "reduce", "options": {"reducers": ["lastNotNull"], "includeTimeField": False}}]
    primary_label = rq.label_names[0]
    return [
        {"id": "reduce",   "options": {"reducers": ["lastNotNull"], "includeTimeField": False, "mode": "seriesToRows"}},
        {"id": "organize", "options": {"renameByName": {"Field": primary_label, "Last *": "value"}}},
    ]


def _layout_positions(pt: str, secondary_count: int) -> dict:
    if pt == "stat":
        w = 24 // max(1, secondary_count + 1)
        return {"main_y": 5, "main_h": 6, "main_w": w, "sec_start_y": 5, "raw_y": 12}
    if pt == "timeseries":
        return {"main_y": 5, "main_h": 10, "main_w": 24, "sec_start_y": 16, "raw_y": 23}
    if pt in ("barchart", "piechart", "bargauge"):
        return {"main_y": 5, "main_h": 10, "main_w": 16, "sec_start_y": 5, "raw_y": 16}
    return {"main_y": 5, "main_h": 10, "main_w": 24, "sec_start_y": 16, "raw_y": 16}


def _build_targets(rq: RegisteredQuery) -> list[dict]:
    targets = []
    for i, (num_col, metric_name) in enumerate(zip(rq.num_cols, rq.metric_names)):
        legend = " | ".join(f"{{{{{ln}}}}}" for ln in rq.label_names) if rq.label_names else num_col
        targets.append({"expr": metric_name, "legendFormat": legend,
                        "refId": chr(65 + i), "instant": False})
    return targets


# ═══════════════════════════════════════════════════════════════════════════════
#  BUILD DASHBOARD
# ═══════════════════════════════════════════════════════════════════════════════

def build_dashboard(
    rq: RegisteredQuery,
    ds_uid: str,
    col_names: list[str],
    rows: list[tuple],
    version: int = 1,
    enrichment: Optional[dict] = None,
    force_panel_type: Optional[str] = None,
    score_result: Optional[dict] = None,
) -> dict:
    pt        = force_panel_type or rq.panel_type
    layout    = _layout_positions(pt, len(rq.secondary_panels))
    unit      = _detect_unit(rq.num_cols[0] if rq.num_cols else "", rq.suggested_unit)
    color_cfg = _color_config(rq)
    x_field   = rq.label_names[0] if rq.label_names else None
    targets   = _build_targets(rq)

    primary_col_idx = col_names.index(rq.num_cols[0]) if rq.num_cols and rq.num_cols[0] in col_names else 0
    thresholds = compute_statistical_thresholds(
        rq.num_cols[0] if rq.num_cols else "", rows, primary_col_idx, rq.domain)

    field_defaults: dict = {"color": color_cfg, "custom": {"fillOpacity": 70}, "unit": unit}
    if thresholds:
        field_defaults["thresholds"] = thresholds
        field_defaults["color"]      = {"mode": "thresholds"}

    enrich        = enrichment or {}
    main_title    = enrich.get("title", rq.title)
    main_desc     = enrich.get("main_description", f"Shows {rq.num_cols[0] if rq.num_cols else 'metric'} for {rq.domain}.")
    sec_descs     = enrich.get("secondary_descriptions", [])
    insights_text = enrich.get("insights", f"• Key metrics for {rq.domain}.")

    template_vars = build_smart_variables(rq.label_names, rq.metric_names, ds_uid)
    drilldown_link = {"title": "View Details", "type": "dashboard", "targetBlank": True, "tags": [rq.domain]}

    # ── Narrative panel (v4.2: show plan narrative if available) ──────────────
    narrative_content = (
        f"**Story:** {rq.plan_narrative}\n\n" if rq.plan_narrative else ""
    )

    up_panel = {
        "type": "stat", "title": f"Scrape Status — {main_title}",
        "description": "1 = UP (last scrape succeeded), 0 = DOWN.",
        "gridPos": {"x": 0, "y": 0, "w": 4, "h": 4},
        "datasource": {"type": "prometheus", "uid": ds_uid},
        "options": {
            "reduceOptions": {"calcs": ["lastNotNull"]}, "colorMode": "background", "graphMode": "none",
            "mappings": [
                {"type": "value", "options": {"1": {"text": "UP",   "color": "green"}}},
                {"type": "value", "options": {"0": {"text": "DOWN", "color": "red"}}},
            ],
        },
        "targets": [{
            "expr": f"nlpg_query_up{{query_id='{rq.query_id}'}}",
            "legendFormat": "status", "refId": "A",
        }],
    }

    cost_panel = {
        "type": "stat", "title": "Est. Query Cost",
        "description": "PostgreSQL EXPLAIN estimated cost. Lower is better.",
        "gridPos": {"x": 4, "y": 0, "w": 4, "h": 4},
        "datasource": {"type": "prometheus", "uid": ds_uid},
        "options": {"reduceOptions": {"calcs": ["lastNotNull"]}, "colorMode": "value"},
        "targets": [{"expr": f"{rq.cost_estimate:.0f}", "legendFormat": "cost", "refId": "A"}],
    }

    info_panel = {
        "type": "text", "title": "Query Info",
        "gridPos": {"x": 8, "y": 0, "w": 12, "h": 4},
        "options": {"mode": "markdown", "content": (
            f"{narrative_content}"
            f"**Question:** {rq.user_request}\n\n"
            f"**Domain:** `{rq.domain}` · **Unit:** `{unit}` · "
            f"**Est. Cost:** `{rq.cost_estimate:.0f}` · **Rows:** `{rq.row_count:,}`\n\n"
            f"**Metrics:** `{', '.join(rq.metric_names)}`\n\n"
            f"**Dashboard v{version}** · Score: `{rq.dashboard_score}/100` · "
            f"Planner confidence: `{rq.plan_confidence:.0%}`\n\n"
            f"```sql\n{rq.sql}\n```"
        )},
    }

    insights_panel = {
        "type": "text", "title": "AI Insights",
        "description": "Business insights generated from the query results.",
        "gridPos": {"x": 20, "y": 0, "w": 4, "h": 4},
        "options": {"mode": "markdown", "content": f"## AI Insights\n\n{insights_text}"},
    }

    main_panel: dict = {
        "title":       main_title,
        "description": main_desc,
        "gridPos":     {"x": 0, "y": layout["main_y"], "w": layout["main_w"], "h": layout["main_h"]},
        "datasource":  {"type": "prometheus", "uid": ds_uid},
        "targets":     targets,
        "fieldConfig": {"defaults": field_defaults, "overrides": []},
        "links":       [drilldown_link],
    }

    if pt == "timeseries":
        main_panel.update({
            "type": "timeseries", "transformations": [],
            "options": {"tooltip": {"mode": "multi"}, "legend": {"displayMode": "table"}},
        })
    elif pt == "barchart":
        main_panel.update({
            "type": "barchart", "transformations": _transforms_category(rq),
            "options": {
                "xField": x_field, "barWidth": 0.7, "xTickLabelRotation": -30,
                "legend": {"displayMode": "list", "placement": "bottom"},
                "tooltip": {"mode": "single"},
            },
        })
    elif pt == "piechart":
        main_panel.update({
            "type": "piechart", "transformations": _transforms_category(rq),
            "options": {
                "pieType": "donut",
                "legend": {"displayMode": "table", "placement": "right"},
                "displayLabels": ["name", "percent"], "tooltip": {"mode": "single"},
            },
        })
    elif pt == "bargauge":
        main_panel.update({
            "type": "bargauge", "transformations": _transforms_category(rq),
            "options": {"orientation": "horizontal", "displayMode": "gradient",
                        "reduceOptions": {"calcs": ["lastNotNull"]}},
        })
    elif pt == "stat":
        main_panel.update({
            "type": "stat", "transformations": _transforms_category(rq),
            "options": {"reduceOptions": {"calcs": ["lastNotNull"]},
                        "colorMode": "background", "sparkline": {"show": True}},
        })
    else:
        main_panel.update({
            "type": "table", "transformations": [{"id": "merge", "options": {}}],
            "options": {"showHeader": True},
        })

    secondary_panels = []
    sec_x_offset = layout["main_w"]
    for i, sec_type in enumerate(rq.secondary_panels[:2]):
        w    = 24 - layout["main_w"] if i == 0 else 12
        x    = sec_x_offset if i == 0 else 0
        y    = layout["sec_start_y"] + (0 if i == 0 else 5)
        desc = sec_descs[i] if i < len(sec_descs) else f"Summary of {rq.domain} metric."
        sp: dict = {
            "title":       f"{main_title} — {sec_type.title()}",
            "description": desc,
            "gridPos":     {"x": x, "y": y, "w": w, "h": 5 if sec_type == "stat" else 6},
            "datasource":  {"type": "prometheus", "uid": ds_uid},
            "targets":     targets,
            "fieldConfig": {"defaults": field_defaults, "overrides": []},
        }
        if sec_type == "stat":
            sp.update({"type": "stat", "transformations": _transforms_category(rq),
                        "options": {"reduceOptions": {"calcs": ["lastNotNull"]}, "colorMode": "background"}})
        elif sec_type == "table":
            sp.update({"type": "table", "transformations": _transforms_raw_table(rq),
                        "options": {"showHeader": True}})
        elif sec_type == "timeseries":
            sp.update({"type": "timeseries", "transformations": [],
                        "options": {"tooltip": {"mode": "multi"}, "legend": {"displayMode": "list"}}})
        else:
            sp.update({"type": "bargauge", "transformations": _transforms_category(rq),
                        "options": {"orientation": "horizontal", "displayMode": "gradient",
                                    "reduceOptions": {"calcs": ["lastNotNull"]}}})
        secondary_panels.append(sp)

    raw_y = (max(p["gridPos"]["y"] + p["gridPos"]["h"] for p in secondary_panels) + 1
             if secondary_panels else layout["raw_y"])
    raw_panel = {
        "type": "table", "title": f"Raw Data — {main_title}",
        "description": "Full query result set for detailed inspection.",
        "gridPos": {"x": 0, "y": raw_y, "w": 24, "h": 10},
        "datasource": {"type": "prometheus", "uid": ds_uid},
        "options": {"showHeader": True},
        "transformations": _transforms_raw_table(rq),
        "targets": targets,
        "fieldConfig": {"defaults": {"unit": unit}, "overrides": []},
    }

    all_panels = (
        [up_panel, cost_panel, info_panel, insights_panel, main_panel]
        + secondary_panels
        + [raw_panel]
    )

    if score_result:
        all_panels.append(build_quality_report_panel(score_result, version, raw_y + 11))

    return {
        "dashboard": {
            "uid":           rq.grafana_uid,
            "title":         f"NL PG — {main_title}",
            "description":   rq.user_request,
            "tags":          rq.tags + [f"v{version}"],
            "timezone":      "browser",
            "refresh":       f"{SCRAPE_INTERVAL}s",
            "time":          {"from": "now-1h", "to": "now"},
            "panels":        all_panels,
            "schemaVersion": 38,
            "templating":    {"list": template_vars},
            "annotations":   {"list": [{
                "builtIn": 1, "datasource": "-- Grafana --",
                "enable": True, "hide": True,
                "iconColor": "rgba(0, 211, 255, 1)",
                "name": "Annotations & Alerts", "type": "dashboard",
            }]},
        },
        "overwrite": True,
        "folderId":  0,
        "message":   f"v1 auto-generated: {rq.user_request[:80]}",
    }


# ═══════════════════════════════════════════════════════════════════════════════
#  PUSH TO GRAFANA — v4.2: retry loop re-invokes planning agent (DPA-2)
# ═══════════════════════════════════════════════════════════════════════════════

_RETRY_PANEL_SEQUENCE = ["barchart", "stat", "table", "bargauge", "timeseries"]


def push_to_grafana(
    rq: RegisteredQuery,
    ds_uid: str,
    col_names: list[str],
    rows: list[tuple],
    enrichment: dict,
) -> str:
    """
    Build → validate → score → regenerate (with planning agent re-invocation)
    → version → push once (overwrite=True).

    DPA-2: On each retry, plan_dashboard() is re-called with force_panel_type
    so that secondary panels, unit, and color are also re-planned — not just
    the primary panel type.
    """
    best_payload      = None
    best_score        = -1
    best_score_result = None
    best_version_num  = 1

    for attempt in range(MAX_REGEN_ATTEMPTS + 1):
        current_version = attempt + 1
        force_pt        = None

        if attempt > 0:
            force_pt = _RETRY_PANEL_SEQUENCE[attempt % len(_RETRY_PANEL_SEQUENCE)]
            log.info("[regen] attempt=%d — re-invoking planning agent with force=%s",
                     attempt + 1, force_pt)

            # DPA-2: Re-run planning agent so secondary panels / unit / color
            # are also reconsidered for the new primary panel type.
            retry_blueprint, retry_plan = plan_dashboard(
                user_request     = rq.user_request,
                sql              = rq.sql,
                col_names        = col_names,
                rows             = rows,
                domain           = rq.domain,
                num_cols         = rq.num_cols,
                str_cols         = rq.str_cols,
                datetime_cols    = rq.datetime_cols,
                has_datetime     = rq.has_datetime,
                force_panel_type = force_pt,
            )
            rq.secondary_panels = retry_blueprint.secondary_panels
            rq.suggested_unit   = retry_blueprint.suggested_unit
            rq.color_scheme     = retry_blueprint.color_scheme
            rq.top_n            = retry_plan.highlight_top_n
            log.info("[regen] Retry plan: primary=%s secondary=%s unit=%s",
                     force_pt, rq.secondary_panels, rq.suggested_unit)

        payload = build_dashboard(
            rq, ds_uid, col_names, rows,
            version=current_version, enrichment=enrichment,
            force_panel_type=force_pt, score_result=None,
        )

        validation_errors = validate_dashboard_json(payload)
        if validation_errors:
            log.warning("[validate] %d issues on attempt %d: %s",
                        len(validation_errors), attempt + 1, validation_errors)
            if attempt < MAX_REGEN_ATTEMPTS:
                continue

        score_result       = score_dashboard(payload["dashboard"]["panels"], rq)
        rq.dashboard_score = score_result["total"]
        log.info("[score] attempt=%d score=%d/100 issues=%s",
                 attempt + 1, score_result["total"], score_result["issues"])

        payload = build_dashboard(
            rq, ds_uid, col_names, rows,
            version=current_version, enrichment=enrichment,
            force_panel_type=force_pt, score_result=score_result,
        )

        version_num = store_dashboard_version(
            rq.grafana_uid, payload, score_result["total"], score_result["issues"])
        rq.current_version = version_num
        payload["message"] = f"v{version_num} auto-generated: {rq.user_request[:80]}"

        if score_result["total"] > best_score:
            best_score        = score_result["total"]
            best_payload      = payload
            best_score_result = score_result
            best_version_num  = version_num

        if score_result["total"] >= DASHBOARD_SCORE_MIN:
            log.info("[score] Target score reached — pushing.")
            break

        if attempt < MAX_REGEN_ATTEMPTS:
            log.warning("[score] Score %d < %d — retrying with different layout.",
                        score_result["total"], DASHBOARD_SCORE_MIN)

    r = requests.post(
        f"{GRAFANA_URL}/api/dashboards/db",
        json=best_payload, auth=_grafana_auth(), headers=_grafana_headers(), timeout=10,
    )
    r.raise_for_status()
    slug = r.json().get("url", f"/d/{rq.grafana_uid}")
    return f"{GRAFANA_URL}{slug}"


# ═══════════════════════════════════════════════════════════════════════════════
#  MAIN REGISTER FLOW — v4.2: plan_dashboard() replaces build_blueprint()
# ═══════════════════════════════════════════════════════════════════════════════

async def register_query(user_request: str) -> Optional[RegisteredQuery]:
    

    loop = asyncio.get_running_loop()
    try:
        log.info("[register] Running multi-agent SQL pipeline…")
        state = await loop.run_in_executor(None, run_sql_pipeline, user_request)

        sql  = state.get("sql", "").strip()
        plan = state.get("plan", "")

        if not sql or (state.get("error") and not state.get("done")):
            log.error("[register] Pipeline failed: %s", state.get("error", "no sql"))
            return None

        upper = sql.upper()
        for kw in FORBIDDEN_SQL:
            if re.search(rf"\b{kw}\b", upper):
                log.error("[register] Blocked keyword: %s", kw)
                return None
        conn=_pg_connect()
        col_names, rows = await execute_query_psycopg2(conn,sql)

        if not rows:
            log.warning("[register] 0 rows returned.")
            return None

        num_cols, str_cols = _classify_columns(rows, col_names)
        if not num_cols:
            log.warning("[register] No numeric columns.")
            return None

        datetime_cols = detect_datetime_columns(col_names, rows)
        has_datetime  = bool(datetime_cols)

        query_id      = hashlib.md5(user_request.encode()).hexdigest()[:8]
        metric_prefix = f"nl_pg_{query_id}"
        label_names   = [_sanitize(c) for c in str_cols[:4]]
        metric_names  = [f"{metric_prefix}_{_sanitize(c)}" for c in num_cols]

        domain     = await loop.run_in_executor(None, classify_intent, user_request, sql)
        log.info("[register] Classified domain: %s", domain)

        kb_pattern = await loop.run_in_executor(None, kb_find_similar, user_request, domain)

        # ── v4.2: Dashboard Planning Agent ────────────────────────────────────
        log.info("[register] Running dashboard planning agent…")
        blueprint, dashboard_plan = await loop.run_in_executor(
            None,
            plan_dashboard,
            user_request, sql, col_names, rows, domain,
            num_cols, str_cols, datetime_cols, has_datetime, kb_pattern,
            None,  # force_panel_type
        )

        panel_type       = blueprint.primary_panel
        secondary_panels = blueprint.secondary_panels
        color_scheme     = blueprint.color_scheme
        suggested_unit   = blueprint.suggested_unit
        tags             = _generate_tags(user_request, domain)

        rq = RegisteredQuery(
            query_id         = query_id,
            user_request     = user_request,
            sql              = sql,
            plan             = plan,
            title            = blueprint.title,
            description      = "",
            panel_type       = panel_type,
            metric_prefix    = metric_prefix,
            num_cols         = num_cols,
            str_cols         = str_cols[:4],
            label_names      = label_names,
            metric_names     = metric_names,
            grafana_uid      = f"nl-pg-{query_id}",
            row_count        = len(rows),
            created_at       = datetime.datetime.now().isoformat(timespec="seconds"),
            cost_estimate    = state.get("cost_estimate", 0.0),
            domain           = domain,
            suggested_unit   = suggested_unit,
            color_scheme     = color_scheme,
            secondary_panels = secondary_panels,
            top_n            = dashboard_plan.highlight_top_n,   # DPA-4
            tags             = tags,
            has_datetime     = has_datetime,
            datetime_cols    = datetime_cols,
            plan_narrative   = dashboard_plan.narrative_focus,   # DPA-3
            plan_confidence  = dashboard_plan.confidence,        # DPA-3
        )

        log.info("[register] Batched LLM enrichment…")
        enrichment = await loop.run_in_executor(
            None,
            generate_enrichment_batch,
            user_request, rq.title, panel_type,
            num_cols[0] if num_cols else "value",
            domain, col_names, rows, secondary_panels,
        )
        rq.title       = enrichment.get("title", rq.title)
        rq.description = enrichment.get("main_description", "")

        push_metrics(rq, col_names, rows)

        log.info("[register] Pushing Grafana dashboard (v4.2)…")
        ds_uid = await loop.run_in_executor(None, get_prometheus_ds_uid)
        rq.grafana_url = await loop.run_in_executor(
            None, push_to_grafana, rq, ds_uid, col_names, rows, enrichment)

        kb_store_pattern(rq, rq.dashboard_score)

        with _registered_lock:
            _registered[query_id] = rq

        # cache_set(user_request, {"query_id": query_id, "grafana_url": rq.grafana_url})

        log.info(
            "[register] Done id=%s rows=%d panel=%s domain=%s "
            "cost=%.0f score=%d/100 version=%d datetime=%s kb_reused=%s "
            "plan_confidence=%.2f narrative=%.60s",
            query_id, len(rows), rq.panel_type, domain,
            rq.cost_estimate, rq.dashboard_score, rq.current_version,
            datetime_cols, bool(kb_pattern),
            rq.plan_confidence, rq.plan_narrative,
        )
        return rq

    except Exception as exc:
        log.exception("[register] Failed: %s", exc)
        return None



app = FastAPI(title="NL→PG Dashboard Agent v4.2")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], allow_methods=["*"], allow_headers=["*"],
)


class RegisterRequest(BaseModel):
    query: str


@app.get("/")
def home():
    return {
        "status":     "NL PG Agent v4.2 running",
        "port":       FASTAPI_PORT,
        "features":   ["dashboard_planning_agent", "rag_kb", "embedding_intent", "score_feedback"],
    }


@app.get("/api/health")
def health():
   
    return {
        "status":              "ok",
        "version":             "4.2",
        "scrape_interval":     SCRAPE_INTERVAL,
        "exporter_port":       EXPORTER_PORT,
        "ollama_model":        OLLAMA_MODEL,
        "ollama_embed_model":  OLLAMA_EMBED_MODEL,
        "max_retries":         MAX_RETRIES,
        "max_query_cost":      MAX_QUERY_COST,
        "dashboard_score_min": DASHBOARD_SCORE_MIN,
        "kb_patterns":         len(_kb_store),
        "domain_embeddings":   len(_domain_embeddings),
        "planner_agent":       "DashboardPlannerAgent (LLM + heuristic fallback)",
    }


@app.post("/api/register")
async def api_register(body: RegisterRequest):
    user_request = body.query.strip()
    if not user_request:
        raise HTTPException(status_code=400, detail="query is required")
    rq = await register_query(user_request)
    if not rq:
        raise HTTPException(status_code=500, detail="Could not register query — check logs")
    return {
        "query_id":         rq.query_id,
        "title":            rq.title,
        "description":      rq.description,
        "panel_type":       rq.panel_type,
        "sql":              rq.sql,
        "plan":             rq.plan,
        "metrics":          rq.metric_names,
        "labels":           rq.label_names,
        "row_count":        rq.row_count,
        "grafana_url":      rq.grafana_url,
        "created_at":       rq.created_at,
        "domain":           rq.domain,
        "cost_estimate":    rq.cost_estimate,
        "secondary_panels": rq.secondary_panels,
        "tags":             rq.tags,
        "color_scheme":     rq.color_scheme,
        "suggested_unit":   rq.suggested_unit,
        "dashboard_score":  rq.dashboard_score,
        "current_version":  rq.current_version,
        "has_datetime":     rq.has_datetime,
        "datetime_cols":    rq.datetime_cols,
        # DPA-3: expose planning metadata
        "dashboard_plan": {
            "panel_type":    rq.panel_type,
            "narrative":     rq.plan_narrative,
            "confidence":    rq.plan_confidence,
            "top_n":         rq.top_n,
        },
    }


@app.get("/api/queries")
def api_queries():
    with _registered_lock:
        return [
            {
                "query_id":         rq.query_id,
                "title":            rq.title,
                "description":      rq.description,
                "user_request":     rq.user_request,
                "panel_type":       rq.panel_type,
                "sql":              rq.sql,
                "plan":             rq.plan,
                "metrics":          rq.metric_names,
                "labels":           rq.label_names,
                "row_count":        rq.row_count,
                "grafana_url":      rq.grafana_url,
                "created_at":       rq.created_at,
                "domain":           rq.domain,
                "cost_estimate":    rq.cost_estimate,
                "secondary_panels": rq.secondary_panels,
                "tags":             rq.tags,
                "color_scheme":     rq.color_scheme,
                "dashboard_score":  rq.dashboard_score,
                "current_version":  rq.current_version,
                "plan_narrative":   rq.plan_narrative,
                "plan_confidence":  rq.plan_confidence,
            }
            for rq in _registered.values()
        ]


@app.get("/api/schema")
def api_schema():
    return {"schema": get_schema()}





@app.get("/api/dashboard/{uid}/history")
def api_dashboard_history(uid: str):
    return get_dashboard_history(uid)


@app.post("/api/dashboard/{uid}/rollback/{version}")
def api_dashboard_rollback(uid: str, version: int):
    payload = rollback_dashboard(uid, version)
    if not payload:
        raise HTTPException(status_code=404, detail=f"Version {version} not found for uid={uid}")
    try:
        r = requests.post(
            f"{GRAFANA_URL}/api/dashboards/db",
            json=payload, auth=_grafana_auth(), headers=_grafana_headers(), timeout=10,
        )
        r.raise_for_status()
        return {"status": "rolled_back", "uid": uid, "version": version, "response": r.json()}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.get("/api/dashboard/{uid}/score")
def api_dashboard_score(uid: str):
    history = get_dashboard_history(uid)
    if not history:
        raise HTTPException(status_code=404, detail="No history found.")
    return history[-1]


@app.get("/api/kb/patterns")
def api_kb_patterns():
    with _kb_lock:
        return [
            {
                "query_hash":       p.query_hash,
                "domain":           p.domain,
                "user_request":     p.user_request,
                "panel_type":       p.panel_type,
                "secondary_panels": p.secondary_panels,
                "score":            p.score,
                "created_at":       p.created_at,
            }
            for p in _kb_store
        ]


@app.get("/api/intent/classify")
async def api_classify_intent(q: str):
    loop = asyncio.get_running_loop()
    domain = await loop.run_in_executor(None, classify_intent, q, "")
    return {"query": q, "domain": domain, "embeddings_active": bool(_domain_embeddings)}


@app.get("/api/planner/preview")
async def api_planner_preview(q: str, domain: str = "sales"):
    """
    Debug endpoint: run only the planning agent and return the plan.
    Useful for testing dashboard layout decisions without full registration.
    """
    loop   = asyncio.get_running_loop()
    agent  = _get_planner_agent()
    plan   = await loop.run_in_executor(
        None, agent.plan,
        q, "", [], [], domain, [], [], [], False, None,
    )
    return {
        "query":              q,
        "domain":             domain,
        "primary_panel_type": plan.primary_panel_type,
        "primary_panel_title":plan.primary_panel_title,
        "primary_panel_reason":plan.primary_panel_reason,
        "secondary_panels":   plan.secondary_panels,
        "secondary_titles":   plan.secondary_titles,
        "color_scheme":       plan.color_scheme,
        "suggested_unit":     plan.suggested_unit,
        "thresholds_strategy":plan.thresholds_strategy,
        "show_timeseries":    plan.show_timeseries,
        "highlight_top_n":    plan.highlight_top_n,
        "narrative_focus":    plan.narrative_focus,
        "confidence":         plan.confidence,
    }


# ── CLI mode ───────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    import sys
    import uvicorn

    if len(sys.argv) > 1 and sys.argv[1] != "serve":
        question = " ".join(sys.argv[1:])
        state = run_sql_pipeline(question)
        print("\n" + "=" * 60)
        print(f"Plan          : {state['plan']}")
        print(f"SQL           : {state['sql']}")
        print(f"Error         : {state['error']}")
        print(f"Retries       : {state['retries']}")
        print(f"Security      : {state.get('security_note', '')}")
        print(f"Cost estimate : {state.get('cost_estimate', 0):.0f}")
        print(f"Plan rows     : {state.get('plan_rows', 0):,}")
        print("=" * 60)
        sys.exit(0)

    uvicorn.run("app:app", host="0.0.0.0", port=FASTAPI_PORT, reload=False)