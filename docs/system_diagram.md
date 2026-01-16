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

  %% Node styling
  classDef external fill:#f4efe6,stroke:#bcae97,color:#2b2418,stroke-width:1px;
  classDef data fill:#e9f2f0,stroke:#6aa5a2,color:#123534,stroke-width:1px;
  classDef hpc fill:#edf1f8,stroke:#6f88b5,color:#1f2d45,stroke-width:1px;
  classDef analytics fill:#f7efe9,stroke:#c58b5a,color:#402812,stroke-width:1px;
  classDef azure fill:#eef6ff,stroke:#6da0d6,color:#0d2a45,stroke-width:1px;
  classDef cd fill:#f2f2f2,stroke:#8a8a8a,color:#2a2a2a,stroke-width:1px;
  classDef users fill:#fff6e1,stroke:#d2a544,color:#3a2a0b,stroke-width:1px;

  class KREST,KWS external;
  class DB,MQ data;
  class REST,WS,WORKER,MIGRATOR hpc;
  class RAG,LLM analytics;
  class PORTAL azure;
  class GHA,FD cd;
  class USERS users;

  %% Subgraph styling
  style External fill:#fbf8f2,stroke:#d7c8b5,stroke-width:1px;
  style Data fill:#f3f8f7,stroke:#9dc3c1,stroke-width:1px;
  style HPC fill:#f3f5fb,stroke:#9db0d4,stroke-width:1px;
  style Analytics fill:#fbf6f1,stroke:#d5aa83,stroke-width:1px;
  style Azure fill:#f2f7ff,stroke:#a7c5e6,stroke-width:1px;
  style CD fill:#f6f6f6,stroke:#bdbdbd,stroke-width:1px;

  %% Link styling
  linkStyle default stroke:#2b2b2b,stroke-width:1.5px;
  linkStyle 11,12,13,15,18 stroke:#8a8a8a,stroke-width:1.2px,stroke-dasharray:5 4;
```
