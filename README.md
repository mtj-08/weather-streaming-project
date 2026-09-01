# ☁️ Real-Time Weather Streaming Pipeline on Azure & Microsoft Fabric

An end-to-end streaming data platform that ingests live weather and air-quality data from a public API, moves it through Azure's event-streaming backbone, lands it in a real-time analytical store on Microsoft Fabric, and surfaces it through a live-refreshing Power BI report — complete with automated email alerting and secure secrets management.

This repository documents the **architecture, design decisions, and reasoning** behind the build. Actual implementation code, notebooks, and the Power BI file live alongside this README — each section below points you to the exact file to open.

---

## Table of Contents

1. [Architecture Overview](#architecture-overview)
2. [Why These Services](#why-these-services)
3. [Data Source: WeatherAPI](#data-source-weatherapi)
4. [Ingestion Layer: Two Approaches Compared](#ingestion-layer-two-approaches-compared)
5. [Security & Governance: Azure Key Vault](#security--governance-azure-key-vault)
6. [Cost Management](#cost-management)
7. [Real-Time Analytics: Microsoft Fabric Eventstream & KQL Database](#real-time-analytics-microsoft-fabric-eventstream--kql-database)
8. [Reporting: Power BI](#reporting-power-bi)
9. [Alerting Pipeline](#alerting-pipeline)
10. [Repository Structure](#repository-structure)
11. [Setup Guide](#setup-guide)

---

## Architecture Overview

The pipeline follows a classic **ingest → stream → store → analyze → alert** flow:

<img width="961" height="502" alt="project2" src="https://github.com/user-attachments/assets/9aee4905-6a47-482d-8090-87ec5e5a7a07" />


Every credential used across this flow (API key, Event Hub connection strings) is centralized in **Azure Key Vault** and accessed through managed identities rather than being hard-coded anywhere, and every resource choice was validated against **Azure Pricing Calculator** estimates before being finalized — these two concerns (security and cost) run through every stage of the design rather than being an afterthought, which is why they get their own dedicated sections below.

---

## Why These Services

| Concern | Service Used | Reasoning |
|---|---|---|
| Live weather data | WeatherAPI.com | Free-tier REST API offering current conditions, forecast, and alerts in one family of endpoints |
| Event ingestion / buffering | Azure Event Hub | Purpose-built for high-throughput event streaming; decouples the producer (weather fetcher) from the consumer (Fabric) |
| Event production | Azure Functions App (chosen) vs Azure Databricks (evaluated) | Both were prototyped; Functions won on cost for this workload — see [Ingestion Layer](#ingestion-layer-two-approaches-compared) |
| Secrets management | Azure Key Vault | Centralizes API keys and connection strings, removes secrets from code, enables fine-grained access via RBAC/ABAC |
| Real-time storage & query | Fabric Eventstream + KQL Database | Native low-latency ingestion from Event Hub with a query language (KQL) purpose-built for time-series and log-style data |
| Visualization | Power BI (via Fabric) | Direct live connection to the KQL database supports true streaming dashboards with sub-minute refresh |
| Alerting | Fabric KQL Queryset + Alert Rule | Lets the alert condition live as a query next to the data, rather than as separate custom code |

---

## Data Source: WeatherAPI

The pipeline pulls three types of data per request cycle, all from the same provider:

- **Current conditions** — temperature, humidity, wind, pressure, cloud cover, UV index, and air-quality sub-readings (CO, NO₂, O₃, SO₂, PM2.5, PM10, plus US-EPA and UK-DEFRA air quality indices).
- **Forecast** — a short multi-day outlook (max/min temperature and condition summary per day).
- **Alerts** — any active severe-weather alerts for the queried location, including headline, severity, description, and recommended action.

These three API responses are fetched independently and then **flattened and merged into a single JSON event** before being pushed downstream — this keeps the schema in Event Hub / KQL simple (one row per event) rather than requiring joins across separate streams. The exact field mapping and merge logic is implemented identically in both ingestion approaches; see `weather-streaming-databricks-notebook` and `function_app_code.py` for the concrete code.

A free trial account on weatherapi.com provides the API key, which is immediately moved into Key Vault rather than being used directly in code (see [Security & Governance](#security--governance-azure-key-vault)).

---

## Ingestion Layer: Two Approaches Compared

Two independent producers were built to push events into Event Hub, evaluated side by side, with one selected for the final deployment.

### Approach 1 — Azure Databricks Notebook

A Databricks cluster runs a Spark structured-streaming job that uses a dummy `rate` source purely as a **scheduling heartbeat** — every micro-batch checks whether 30 seconds have elapsed since the last successful send, and if so, calls the WeatherAPI endpoints, flattens the response, and publishes it to Event Hub using the `azure-eventhub` Python SDK. Checkpointing is handled through a dedicated storage container (via an access connector and storage credential) so the stream can recover cleanly on restart.

Authentication to Event Hub and to the WeatherAPI key is done entirely through **Databricks secret scopes backed by Key Vault** — no credential is ever pasted into notebook code.

Logic to review: `weather-streaming-databricks-notebook`

<img width="2068" height="1239" alt="image" src="https://github.com/user-attachments/assets/c23e3194-9714-4a30-859d-73735cced713" />


### Approach 2 — Azure Functions App (Timer Trigger)

A Python Functions App, built and deployed from VS Code, uses a **timer trigger** with a cron expression firing every 30 seconds. On each execution it authenticates using the Function App's **system-assigned managed identity** (via `DefaultAzureCredential`), retrieves the WeatherAPI key from Key Vault at runtime, fetches and flattens the same three API responses, and publishes the event directly to Event Hub — again using identity-based auth rather than a stored connection string, via the `azure-identity`, `azure-eventhub`, and `azure-keyvault-secrets` libraries.

Logic to review: `function_app_code.py`

<img width="1956" height="1188" alt="image" src="https://github.com/user-attachments/assets/892bc15e-c274-4aa4-a36c-51beb1e97b05" />

### The Decision

Both approaches were run and priced out using the Azure Pricing Calculator. For a lightweight, single-event-every-30-seconds workload with no heavy transformation, **Databricks' always-on cluster cost far outweighs its benefit**, whereas a **Consumption-plan Functions App bills only for the few seconds of actual execution per run**. Databricks remains the better tool when the workload involves large-scale data processing or Spark-based transformation — just not for this simple ingestion pattern. The Functions App was therefore selected as the production ingestion method.

---

## Security & Governance: Azure Key Vault

Key Vault is the backbone of secret handling across the entire pipeline and deserves emphasis as a first-class component, not a side detail:

- **What's stored**: the WeatherAPI key and the Event Hub connection string (used only where identity-based auth wasn't the access path).
- **Soft delete + purge protection**: enabled so deleted secrets/vaults are recoverable, and so the vault can be purged immediately when needed instead of waiting out the default retention window.
- **Access model — Azure RBAC over legacy access policies**: every consumer of a secret (a person, or a compute resource's managed identity) is granted a scoped role rather than broad vault access:
  - *Key Vault Secrets Officer* — granted to the human setting up secrets (create/manage secret values).
  - *Key Vault Secrets User* — granted to Azure Databricks' identity and to the Functions App's managed identity, allowing them to **read** secrets at runtime but not manage them.
  - Databricks additionally required a secret scope linked to the vault's DNS name and resource ID (via the `#secrets/createScope` admin URL) before it could read secrets through `dbutils.secrets.get`.
- **Why this matters**: no API key or connection string appears anywhere in notebook code, function code, or version control. Access is auditable per-identity, and revoking access is a single role-assignment removal rather than a credential rotation across multiple files.

<img width="1772" height="1186" alt="image" src="https://github.com/user-attachments/assets/68abeb58-bc04-46b0-9d44-4ff32a633aac" />

---

## Cost Management

Cost was treated as a design input, not a post-hoc check:

- The **Azure Pricing Calculator** was used up front to model both ingestion approaches (Databricks always-on compute vs. Functions Consumption plan) under the same event frequency, which directly drove the decision documented above.
- **Consumption-based hosting** was chosen wherever possible (Functions App Consumption plan) so cost scales with actual invocations rather than idle uptime.
- **Basic tier** was selected for Event Hub namespace and default throughput units, since this is a low-volume, single-producer workload.
- **Fabric capacity (F2, trial)** was deliberately sized at the smallest available SKU, and the capacity is **paused when not actively in use** to avoid idle billing — Fabric capacity bills by uptime regardless of query volume, so pausing it is the single most effective cost lever available for this resource.
- Together, these choices keep the always-on portion of the architecture (Event Hub + Fabric) minimal while the bursty portion (ingestion) is fully consumption-billed.

---

## Real-Time Analytics: Microsoft Fabric Eventstream & KQL Database

Once events land in Event Hub, Fabric takes over as the real-time analytical layer:

1. **Eventstream** is the Fabric item that subscribes to Event Hub as a source. A **shared access policy scoped to Listen-only** permissions is used for this connection, keeping Fabric's access read-only and separate from the Send policy used by the producers.
2. The Eventstream routes incoming events into an **Eventhouse**, landing them in a **KQL database** table — this is what gives the pipeline sub-second query latency on freshly arrived data, which a traditional data warehouse load pattern wouldn't support.
3. From here, data is queried directly with **Kusto Query Language (KQL)** — both for ad-hoc exploration (e.g., pulling the latest 100 rows) and for the alert logic described below.

Refer to the KQL used for alerting in `KQL_queryset.txt` for the concrete query pattern.

<img width="2272" height="700" alt="image" src="https://github.com/user-attachments/assets/6de3a7c7-f6c4-405a-9813-d5f4454d40b5" />

---

## Reporting: Power BI

Two complementary reporting paths were built on top of the KQL database:

- **Fabric-native streaming report**: built directly against the Eventstream/KQL table inside Fabric, with **Auto Page Refresh set to a 30-second interval**, giving a genuinely live dashboard with no manual refresh needed.
- **Power BI Desktop model** (`.pbix`, attached to this repo): connects to the KQL database and defines two logical datasets — **historical weather data** and **latest weather data** — so trend visuals and "current conditions" visuals can be built from the same source without conflicting refresh needs.
- A calculated **Air Quality Band** column was added via DAX, bucketing the UK-DEFRA air quality index (1–10) into **Low / Moderate / High / Very High** categories using a `SWITCH` expression — this turns a numeric index that's hard to interpret at a glance into a plain-language label suitable for a dashboard visual or alert message.

Open the `.pbix` file in Power BI Desktop to see the full data model, relationships, and visuals.

<img width="1970" height="1104" alt="image" src="https://github.com/user-attachments/assets/1748cbe3-8201-4837-a08d-cf8c9b21f7a2" />


---

## Alerting Pipeline

Alerting is implemented entirely as a **query + rule**, without any custom alerting code:

1. A **KQL queryset** filters the weather table down to rows where the `alerts` field is non-empty (i.e., WeatherAPI has reported an active alert for the location).
2. The query groups by the alert content and tracks the **most recent time each distinct alert was seen** (`max(EventProcessedUtcTime)`).
3. A **left-anti join** against the same query — filtered to alerts last seen more than a minute ago — is used to isolate alerts that are effectively **new or newly re-triggered**, preventing the same ongoing alert from re-firing a notification every refresh cycle.
4. This queryset is wired to a **Fabric alert rule**, which is configured to send an **email notification** to the recipient whenever the query returns a new matching row.
5. The logic was validated end-to-end by manually injecting a test event into Event Hub with an alert payload of `"Test Alert"` and confirming the email fired as expected.

The full query is in `KQL_queryset.txt`.

<img width="1728" height="992" alt="image" src="https://github.com/user-attachments/assets/a19b7a10-9a43-4d15-9d27-345a3a573ea0" />


---

## Repository Structure

```
├── README.md                              # this file
├── weather-streaming-databricks-notebook  # Databricks ingestion approach (Spark structured streaming)
├── function_app_code.py                   # Azure Functions ingestion approach (timer trigger)
├── KQL_queryset.txt                       # KQL alert detection query
└── live_weather_report_PBI.pbix                     # Power BI report (historical + live weather datasets)
```

---

## Setup Guide

A condensed version of the resource provisioning order used to stand this project up:

1. **Resource Group** — create a dedicated resource group for all project resources.
2. **WeatherAPI.com** — sign up for a free-tier account and obtain an API key.
3. **Azure Key Vault** — create the vault (Standard tier, RBAC enabled, purge protection on), then store the WeatherAPI key as a secret.
4. **Azure Event Hub** — create a namespace (Basic tier, default throughput) and an Event Hub instance inside it; create a Send shared access policy for producers and a Listen policy for Fabric.
5. **Ingestion compute** — provision *either or both*:
   - **Azure Databricks** (Premium tier) with a secret scope linked to Key Vault, and the `azure-eventhub` package installed on the cluster.
   - **Azure Functions App** (Python, Linux, Consumption plan) with a system-assigned managed identity, granted `Key Vault Secrets User` and `Azure Event Hubs Data Sender` roles.
6. **Microsoft Fabric** — start a trial, provision an F2 capacity, and create a workspace.
7. **Fabric Eventstream + Eventhouse/KQL database** — connect the Eventstream to the Event Hub using the Listen policy, and route data into a KQL database table.
8. **Power BI** — build a Fabric-native streaming report (30-second auto-refresh) and/or connect Power BI Desktop to the KQL database for deeper modeling.
9. **Alerting** — build the KQL queryset described above and attach a Fabric alert rule with email notification.
10. **Cost check** — before finalizing, run the full resource list through the Azure Pricing Calculator and pause/scale down anything (especially Fabric capacity) not actively in use.

**Azure resources**

<img width="2560" height="1308" alt="image" src="https://github.com/user-attachments/assets/f69c60d1-f686-47ad-90cd-04833041ae3c" />

**Fabric resources**

<img width="2396" height="1052" alt="image" src="https://github.com/user-attachments/assets/a488207e-f024-4e7b-943d-2498339a6312" />


For the exact click-by-click configuration and full code, refer to the individual files listed in [Repository Structure](#repository-structure).
