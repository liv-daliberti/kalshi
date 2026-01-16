# System diagram

This diagram shows the main services, data stores, and deployment paths.
Solid lines are required flows; dashed lines are optional integrations.

```mermaid
flowchart LR
  %% External APIs
  subgraph External["External APIs"]
    KREST["Kalshi REST API\n(events, markets, candlesticks)"]
    KWS["Kalshi WS API\n(ticker, lifecycle)"]
  end

  %% Data + queue
  subgraph Data["Data Layer"]
    DB["PostgreSQL\n(events, markets, ticks, candles, ingest_state)\n(Apptainer instance or external host)"]
    MQ["RabbitMQ\n(optional work-queue notifications)"]
  end

  %% Ingestion services
  subgraph HPC["HPC / SLURM Job (Apptainer)"]
    REST["rest_service\n(discovery + backfill scheduler)"]
    WS["ws_service\n(ticks + lifecycle)"]
    WORKER["worker_service\n(backfill workers)"]
    MIGRATOR["migrator\n(schema init/update)"]
  end

  %% Optional analytics
  subgraph Analytics["Optional Analytics"]
    RAG["rag_service\n(predictions)"]
    LLM["LLM / Embeddings Provider\n(optional external)"]
  end

  %% Portal + deployment
  subgraph Azure["Azure App Service (Linux)"]
    PORTAL["web_portal\n(read-only UI)"]
  end

  subgraph CD["CI/CD + Edge"]
    GHA["GitHub Actions\n(build + deploy)"]
    FD["Azure Front Door\n(optional public edge)"]
  end

  USERS["Users / Browsers"]

  %% External API flows
  KREST --> REST
  KREST --> WORKER
  KWS --> WS

  %% DB flows
  REST -->|upsert events/markets| DB
  REST -->|enqueue backfill jobs| DB
  WS -->|ticks + lifecycle| DB
  DB -->|active_markets| WS
  WORKER -->|candlesticks| DB
  MIGRATOR -->|schema| DB
  RAG -->|read/write predictions| DB
  PORTAL -->|read-only queries| DB

  %% Queue notifications (optional)
  REST -.->|publish notifications| MQ
  MQ -.->|notify workers| WORKER

  %% RAG external providers (optional)
  RAG -.->|embeddings/prompts| LLM

  %% Portal edge + CI/CD
  GHA -->|deploy| PORTAL
  GHA -.->|configure| FD
  USERS --> FD
  FD --> PORTAL
  USERS -.->|direct (no Front Door)| PORTAL
```
