# NL PostgreSQL Dashboard Generator

> An LLM-powered analytics platform that converts natural-language questions into validated PostgreSQL queries, analyzes the result, and automatically generates Grafana dashboards with Prometheus-backed metrics.

## Overview

**NL PostgreSQL Dashboard Generator** is a full-stack analytics automation project designed to reduce the manual work involved in turning a business question into an operational dashboard.

Instead of writing SQL and configuring Grafana panels manually, a user can enter a question such as:

```text
Show monthly sales revenue for the last 12 months
```

The system then:

1. Reads the PostgreSQL database schema.
2. Uses an Ollama-hosted LLM to reason about the request.
3. Generates PostgreSQL SQL using only the available schema.
4. Validates the generated SQL for safety and correctness.
5. Retries/improves the query when validation or execution fails.
6. Executes the query and analyzes the returned columns and values.
7. Classifies the request/domain and selects an appropriate dashboard design.
8. Plans visualization panels using an LLM-assisted dashboard planner with heuristic fallbacks.
9. Scores and validates the generated dashboard.
10. Publishes the dashboard to Grafana.
11. Exposes query results as Prometheus metrics.
12. Continuously refreshes registered queries through a background scrape loop.
13. Maintains dashboard versions and supports rollback.

The project combines **LLMs, LangGraph, PostgreSQL, FastAPI, Grafana, Prometheus, Redis, SQLGlot, and Docker Compose** into one automated analytics workflow.

---

## Key Features

### Natural Language → SQL

Users can ask questions in natural language instead of manually writing SQL.

The SQL generation pipeline uses:

- PostgreSQL schema introspection
- LLM-based query planning
- LLM-based SQL generation
- SQL safety checks
- LLM-based SQL review
- Automatic query improvement/retry
- PostgreSQL statement timeout
- Query cost and row-limit controls

Only read-oriented SQL is intended to be accepted. Dangerous operations such as `DELETE`, `DROP`, `TRUNCATE`, `UPDATE`, `INSERT`, `ALTER`, `CREATE`, `GRANT`, and `REVOKE` are explicitly blocked.

### Automated Dashboard Planning

The system analyzes query results and determines:

- Dimension/label columns
- Numeric/metric columns
- Date/time columns
- Suggested visualization type
- Units
- Top-N behavior
- Secondary panels
- Tags
- Color scheme
- Dashboard layout
- Dashboard metadata

### Grafana Integration

Generated dashboards are pushed directly to Grafana through its HTTP API.

The application can:

- Detect the Prometheus datasource
- Generate Grafana dashboard JSON
- Create panels and layouts
- Configure visualization units
- Add transformations
- Add dashboard variables
- Publish dashboards
- Maintain dashboard versions
- Retrieve dashboard history
- Roll back to previous versions

### Prometheus Metrics

Registered SQL queries are periodically executed by a background scraper.

Query results are converted into Prometheus-compatible metrics and exposed through the exporter endpoint.

Default exporter port:

```text
9877
```

Prometheus then scrapes the exporter and Grafana can visualize those metrics.

### Knowledge Base / RAG-like Pattern Reuse

The application maintains dashboard patterns and domain information that can be reused for future requests.

It includes:

- Domain classification
- Embedding-based similarity
- Stored dashboard patterns
- Similar-pattern lookup
- Dashboard score feedback

This allows previously successful dashboard structures to influence future dashboard planning.

### Dashboard Quality Scoring

Generated dashboards are automatically evaluated using a dashboard scoring system.

The configured minimum score is controlled by:

```text
DASHBOARD_SCORE_MIN=80
```

The system can use the score and validation issues to decide whether regeneration is necessary.

### Versioning and Rollback

Dashboard versions are stored so that generated dashboards can be inspected and previous versions can be restored.

Available operations include:

- View dashboard history
- View dashboard score
- Roll back to a previous version

### Web Interface

The project includes a custom frontend in `index.html` for interacting with the dashboard generator.

The UI provides a dashboard-oriented interface for:

- Entering natural-language queries
- Viewing generated SQL
- Viewing planning information
- Viewing dashboard results
- Inspecting system status
- Interacting with generated dashboards

---

## Architecture

```text
                    ┌─────────────────────────┐
                    │       Web UI             │
                    │       index.html         │
                    └────────────┬────────────┘
                                 │
                                 ▼
                    ┌─────────────────────────┐
                    │       FastAPI            │
                    │        app.py            │
                    └────────────┬────────────┘
                                 │
               ┌─────────────────┼──────────────────┐
               │                 │                  │
               ▼                 ▼                  ▼
      ┌────────────────┐ ┌───────────────┐ ┌─────────────────┐
      │ SQL Pipeline   │ │ Dashboard     │ │ Knowledge Base  │
      │                │ │ Planner       │ │ / Embeddings    │
      │ Reason         │ │               │ │                 │
      │ Generate       │ │ Intent        │ │ Patterns        │
      │ Validate       │ │ Panel type    │ │ Similarity      │
      │ Improve        │ │ Layout        │ │ Domain vectors  │
      │ Execute        │ │ Scoring       │ │                 │
      └───────┬────────┘ └───────┬───────┘ └─────────────────┘
              │                  │
              ▼                  ▼
       ┌───────────────┐  ┌────────────────┐
       │   Ollama      │  │    Grafana     │
       │ Llama model   │  │ Dashboards     │
       │ Embeddings    │  │ Visualization  │
       └───────────────┘  └───────┬────────┘
                                  │
                                  ▼
                         ┌────────────────┐
                         │  Prometheus    │
                         │ Metrics        │
                         └───────┬────────┘
                                 │
                                 ▼
                         ┌────────────────┐
                         │ PostgreSQL     │
                         │ Analytics DB   │
                         └────────────────┘

                         ┌────────────────┐
                         │     Redis      │
                         │ Cache / state  │
                         └────────────────┘
```

---

## Technology Stack

| Component | Technology |
|---|---|
| Backend | Python |
| API | FastAPI |
| LLM orchestration | LangChain + LangGraph |
| Local LLM runtime | Ollama |
| Default LLM | Llama 3 |
| Embeddings | `nomic-embed-text` |
| Database | PostgreSQL 16 |
| SQL validation | SQLGlot + rule-based checks |
| Dashboard | Grafana |
| Metrics | Prometheus |
| Cache | Redis |
| Containerization | Docker + Docker Compose |
| Data access | Psycopg2 / AsyncPG |
| Validation models | Pydantic |
| Metrics exporter | `prometheus-client` |
| Frontend | HTML / CSS / JavaScript |

---

## Project Structure

```text
DashboardGenerator/
│
├── app.py
│   └── Main FastAPI application and dashboard-generation pipeline
│
├── app_clone.py
│   └── Alternate/clone application version
│
├── final.py
│   └── Alternate consolidated application version
│
├── lohith.py
│   └── Additional development/version of the application
│
├── index.html
│   └── Frontend web interface
│
├── requirements.txt
│   └── Python dependencies
│
├── Dockerfile
│   └── Container image definition for the FastAPI application
│
├── docker-compose.yml
│   └── Multi-service development/deployment environment
│
├── prometheus.yml
│   └── Prometheus scraping configuration
│
└── try.ipynb
    └── Development/experimentation notebook
```

> `app.py` is the primary application entry point in the supplied project. The other Python files appear to be alternate/development versions.

---

# Running the Project

## Option 1 — Docker Compose

Docker Compose is the recommended way to start the complete stack.

### Prerequisites

Install:

- Docker Desktop
- Docker Compose

Verify:

```bash
docker --version
docker compose version
```

### Start the Services

From the project directory:

```bash
docker compose up --build
```

This starts:

| Service | Default Port | Purpose |
|---|---:|---|
| PostgreSQL | `5432` | Analytics database |
| Redis | `6379` | Cache/state |
| Ollama | `11434` | Local LLM inference |
| FastAPI application | `5050` | Backend/API |
| Prometheus | `9090` | Metrics collection |
| Grafana | `3000` | Dashboard visualization |
| Metrics exporter | `9877` | Prometheus scrape endpoint |

### Stop the Stack

```bash
docker compose down
```

To remove persistent Docker volumes as well:

```bash
docker compose down -v
```

**Warning:** removing volumes deletes persisted PostgreSQL/Grafana/Ollama container data.

---

## Pull the Ollama Models

The application defaults to:

```text
OLLAMA_MODEL=llama3
OLLAMA_EMBED_MODEL=nomic-embed-text
```

After starting Ollama, pull the required models:

```bash
docker exec -it ollama ollama pull llama3
docker exec -it ollama ollama pull nomic-embed-text
```

Check installed models:

```bash
docker exec -it ollama ollama list
```

If your Ollama installation uses a different model, set:

```text
OLLAMA_MODEL=<your-model>
OLLAMA_EMBED_MODEL=<your-embedding-model>
```

---

# Accessing the Application

After startup:

### Dashboard Generator

```text
http://localhost:5050
```

### FastAPI Health Check

```text
http://localhost:5050/api/health
```

### Grafana

```text
http://localhost:3000
```

Default credentials configured by the Compose file:

```text
Username: admin
Password: admin
```

Change these credentials for any non-local/development deployment.

### Prometheus

```text
http://localhost:9090
```

---

# Example Workflow

Suppose PostgreSQL contains:

```text
sales
├── order_id
├── order_date
├── region
├── product
└── revenue
```

A user enters:

```text
Show monthly revenue by region for the last 12 months
```

The application performs approximately the following flow:

```text
Natural-language question
        │
        ▼
Schema inspection
        │
        ▼
LLM reasoning / query plan
        │
        ▼
SQL generation
        │
        ▼
SQL safety validation
        │
        ▼
LLM SQL review
        │
        ├── Invalid ──► Improve / Retry
        │
        ▼
PostgreSQL execution
        │
        ▼
Result analysis
        │
        ▼
Intent / domain classification
        │
        ▼
Dashboard planning
        │
        ▼
Dashboard quality scoring
        │
        ▼
Grafana dashboard generation
        │
        ▼
Prometheus metric registration
        │
        ▼
Grafana visualization
```

---

# API Reference

## Health

```http
GET /api/health
```

Returns application status and configuration information.

Example:

```bash
curl http://localhost:5050/api/health
```

---

## Register a Natural-Language Query

```http
POST /api/register
```

Request:

```json
{
  "query": "Show monthly sales revenue for the last 12 months"
}
```

Example:

```bash
curl -X POST http://localhost:5050/api/register \
  -H "Content-Type: application/json" \
  -d "{\"query\":\"Show monthly sales revenue for the last 12 months\"}"
```

The response contains information such as:

```json
{
  "query_id": "...",
  "title": "...",
  "description": "...",
  "panel_type": "...",
  "sql": "...",
  "plan": "...",
  "metrics": [],
  "labels": [],
  "row_count": 0,
  "grafana_url": "...",
  "domain": "...",
  "dashboard_score": 0,
  "current_version": 1,
  "dashboard_plan": {
    "panel_type": "...",
    "narrative": "...",
    "confidence": 0.0,
    "top_n": 0
  }
}
```

---

## List Registered Queries

```http
GET /api/queries
```

Example:

```bash
curl http://localhost:5050/api/queries
```

---

## Inspect Database Schema

```http
GET /api/schema
```

Example:

```bash
curl http://localhost:5050/api/schema
```

---

## Dashboard History

```http
GET /api/dashboard/{uid}/history
```

Returns stored versions for a dashboard.

---

## Dashboard Rollback

```http
POST /api/dashboard/{uid}/rollback/{version}
```

Example:

```bash
curl -X POST http://localhost:5050/api/dashboard/<UID>/rollback/2
```

---

## Dashboard Score

```http
GET /api/dashboard/{uid}/score
```

Returns the dashboard quality/validation information.

---

## Knowledge Base Patterns

```http
GET /api/kb/patterns
```

Returns stored dashboard patterns.

---

## Intent Classification

```http
GET /api/intent/classify
```

The endpoint is used to classify a natural-language request into an analytics domain.

---

## Dashboard Planner Preview

```http
GET /api/planner/preview
```

Used to inspect dashboard planning behavior before/without the complete dashboard publishing workflow.

---

# Configuration

The application is configured primarily through environment variables.

## Database

```text
PG_HOST=localhost
PG_PORT=5432
PG_DB=postgres
PG_USER=postgres
PG_PASS=password

DATABASE_URL=postgresql://postgres:password@localhost:5432/postgres
```

When running inside Docker Compose, the application connects to PostgreSQL using the service name:

```text
postgresql://postgres:password@postgres:5432/postgres
```

---

## Ollama

```text
OLLAMA_HOST=http://ollama:11434
OLLAMA_MODEL=llama3
OLLAMA_EMBED_MODEL=nomic-embed-text
```

---

## Grafana

```text
GRAFANA_URL=http://grafana:3000
GRAFANA_USER=admin
GRAFANA_PASS=admin
GRAFANA_API_KEY=
PROMETHEUS_DS_NAME=Prometheus
```

For production, prefer an API key or another secure authentication mechanism instead of storing default credentials.

---

## Prometheus / Scraping

```text
EXPORTER_PORT=9877
SCRAPE_INTERVAL=30
```

`SCRAPE_INTERVAL` determines how frequently registered SQL queries are executed and their results exposed as metrics.

---

## Query Safety / Limits

```text
MAX_RETRIES=3
MAX_QUERY_COST=500000
MAX_QUERY_ROWS=1000000
```

These settings are intended to prevent expensive or uncontrolled analytical queries.

---

## Dashboard Quality

```text
DASHBOARD_SCORE_MIN=80
MAX_REGEN_ATTEMPTS=2
```

The application can regenerate a dashboard when its quality score does not satisfy the configured threshold.

---

## Redis

```text
REDIS_URL=redis://redis:6379/0
CACHE_TTL=3600
```

---

# SQL Safety

The system includes multiple layers of protection around generated SQL.

### 1. Schema-Constrained Generation

The LLM receives the actual PostgreSQL schema and is instructed to use only existing tables and columns.

### 2. Forbidden SQL Detection

The application blocks dangerous statements containing keywords such as:

```text
DELETE
DROP
TRUNCATE
UPDATE
INSERT
ALTER
CREATE
GRANT
REVOKE
VACUUM
REINDEX
```

### 3. LLM Review

A second validation stage reviews the generated query for:

- Incorrect table names
- Incorrect column names
- Invalid aliases
- Missing joins
- Potential syntax errors

### 4. PostgreSQL Timeout

Database connections use a statement timeout to prevent an individual query from running indefinitely.

### 5. Query Limits

The application maintains configurable query-cost and row-count limits.

> These protections are application-level safeguards and should not be treated as a complete database security boundary. Production deployments should also use a dedicated read-only PostgreSQL user with appropriate database permissions.

---

# Prometheus Flow

The project exposes query-derived metrics through a Prometheus exporter.

```text
PostgreSQL
    │
    ▼
Registered SQL Query
    │
    ▼
Background Scraper
    │
    ▼
Metric Conversion
    │
    ▼
FastAPI / Prometheus Exporter
    │
    ▼
Prometheus
    │
    ▼
Grafana
```

Prometheus is configured to scrape:

```text
nl_pg_exporter
```

through:

```text
host.docker.internal:9877
```

with a 5-second Prometheus scrape interval.

The application itself periodically refreshes registered queries according to:

```text
SCRAPE_INTERVAL
```

---

# Development Without Docker

If you want to run the FastAPI application directly on the host:

## 1. Create a Virtual Environment

```bash
python -m venv .venv
```

Windows:

```powershell
.venv\Scripts\activate
```

Linux/macOS:

```bash
source .venv/bin/activate
```

## 2. Install Dependencies

```bash
pip install -r requirements.txt
```

## 3. Start PostgreSQL, Redis, Ollama, Prometheus, and Grafana

These services must be running separately.

## 4. Start Ollama

Make sure the configured models are available:

```bash
ollama pull llama3
ollama pull nomic-embed-text
```

## 5. Start FastAPI

```bash
python app.py
```

Alternatively:

```bash
uvicorn app:app --host 0.0.0.0 --port 8096
```

> When running outside Docker, make sure `DATABASE_URL`, `OLLAMA_HOST`, `GRAFANA_URL`, and `REDIS_URL` point to services accessible from the host machine.

---

# Example Environment File

Create a `.env` file for local development:

```env
PG_HOST=localhost
PG_PORT=5432
PG_DB=postgres
PG_USER=postgres
PG_PASS=password

DATABASE_URL=postgresql://postgres:password@localhost:5432/postgres

OLLAMA_HOST=http://localhost:11434
OLLAMA_MODEL=llama3
OLLAMA_EMBED_MODEL=nomic-embed-text

GRAFANA_URL=http://localhost:3000
GRAFANA_USER=admin
GRAFANA_PASS=admin
GRAFANA_API_KEY=

REDIS_URL=redis://localhost:6379/0

EXPORTER_PORT=9877
SCRAPE_INTERVAL=30

MAX_RETRIES=3
MAX_QUERY_COST=500000
MAX_QUERY_ROWS=1000000

DASHBOARD_SCORE_MIN=80
MAX_REGEN_ATTEMPTS=2
CACHE_TTL=3600

PROMETHEUS_DS_NAME=Prometheus
```

Do **not** commit real passwords, API keys, tokens, or production connection strings to GitHub.

---

# Troubleshooting

## Ollama model not found

Check:

```bash
docker exec -it ollama ollama list
```

Pull the required model:

```bash
docker exec -it ollama ollama pull llama3
```

---

## PostgreSQL connection error

Check that PostgreSQL is running:

```bash
docker compose ps
```

Check the database container logs:

```bash
docker logs postgres
```

Verify the connection string.

Inside Docker Compose, use:

```text
postgres
```

as the PostgreSQL hostname rather than `localhost`.

---

## Grafana dashboard is not created

Check:

```bash
docker logs grafana
docker logs nl-pg-agent
```

Verify:

```text
GRAFANA_URL
GRAFANA_USER
GRAFANA_PASS
```

Also make sure the Prometheus datasource exists in Grafana.

---

## Prometheus shows no metrics

Check the exporter:

```text
http://localhost:9877/metrics
```

Then open Prometheus:

```text
http://localhost:9090
```

Verify that the exporter target is healthy.

---

## Docker containers are running but the application cannot connect

Remember that inside Docker:

```text
localhost
```

means the current container.

Use Docker Compose service names instead:

```text
postgres
redis
ollama
grafana
prometheus
```

---

# Security Recommendations

Before using this project beyond local development:

- Use a dedicated PostgreSQL read-only user.
- Do not use the default PostgreSQL password.
- Do not use the default Grafana password.
- Store secrets in environment variables or a secret manager.
- Do not commit `.env` files containing credentials.
- Restrict Grafana and Prometheus network exposure.
- Add authentication and authorization to the API.
- Apply stricter SQL parsing/AST validation in production.
- Set appropriate database statement and resource limits.
- Restrict which schemas/tables the LLM can access.
- Use HTTPS when exposing the service outside localhost.
- Rotate API keys and credentials regularly.

---

# Why This Project?

Traditional dashboard creation often requires several manual steps:

```text
Business Question
      ↓
Understand Database
      ↓
Write SQL
      ↓
Validate SQL
      ↓
Analyze Result
      ↓
Choose Chart
      ↓
Configure Grafana
      ↓
Configure Metrics
      ↓
Build Dashboard
```

This project attempts to automate that workflow:

```text
Business Question
      ↓
AI Dashboard Generator
      ↓
SQL + Analysis + Visualization Planning
      ↓
Grafana Dashboard
```

The goal is to provide a more accessible interface for analytics while retaining explicit validation and observability mechanisms.

---

# Future Improvements

Potential extensions include:

- Multi-database support
- PostgreSQL read-only role provisioning
- Better SQL AST validation
- Authentication and role-based access control
- Streaming query execution
- More visualization types
- Advanced dashboard templates
- Human approval before publishing dashboards
- Improved dashboard quality evaluation
- Persistent knowledge-base storage
- More sophisticated RAG over historical dashboards
- Query performance monitoring
- Distributed background workers
- Kubernetes deployment
- Automated tests and CI/CD
- OpenTelemetry tracing
- Fine-grained audit logging

---

# Project Highlights

This project demonstrates practical integration of:

- **Generative AI**
- **Natural-language interfaces**
- **Text-to-SQL**
- **LLM agents**
- **LangGraph workflows**
- **PostgreSQL**
- **SQL validation**
- **Data analytics**
- **Automated dashboard generation**
- **Grafana**
- **Prometheus**
- **Redis**
- **FastAPI**
- **Docker**
- **Observability**

It is particularly suitable as a demonstration project for **Data Engineering, Analytics Engineering, ML Engineering, AI Engineering, and TPM/data-platform-oriented roles**.

---

## Acknowledgements

This project uses the following open-source technologies:

- FastAPI
- LangChain
- LangGraph
- Ollama
- PostgreSQL
- Grafana
- Prometheus
- Redis
- SQLGlot
- Docker
- Python
