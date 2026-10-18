# Project 2 Architecture & Design Doc: Real-Time Streaming & Medallion Lakehouse Pipeline (Redpanda + PySpark + Delta Lake)

> **Superseded by `project_2_architecture_design_doc_v2.md`.** Note the delivered
> implementation deviates from this v1's **10-minute** watermark: a live run proved
> 10 min drops valid late events (the producer delays up to 15 min), so the shipped
> `WATERMARK` is **20 minutes**. See the implementation note in the v2 doc and
> `docs/CONCEPTS_AND_DESIGN_DECISIONS.md` §1.4.

## 1. Project Overview & Business Value

### **Project Title**
**Real-Time E-Commerce Clickstream & Order Events Ingestion Lakehouse**

### **Business Problem / Value Proposition**
In modern e-commerce and digital operations, traditional batch ETL pipelines create an operational data lag of hours or days. Business stakeholders—such as Fraud Prevention, Inventory Logistics, and Customer Retention teams—require low-latency streaming data to detect checkout anomalies, prevent overselling stock during high-traffic events, and monitor immediate conversion drops.

However, streaming processing introduces specific engineering challenges:
* **Network & Mobile Retries:** Mobile devices and browser networks frequently resend payloads, producing duplicate event streams.
* **Late-Arriving Events:** Out-of-order event arrivals due to network latency disrupt chronological processing and aggregate metrics.
* **The "Small File Problem":** Continuous micro-batch writes to object stores create thousands of tiny files that degrade query performance over time.

### **The Solution & Impact**
This project constructs a **Medallion Lakehouse Architecture (Bronze → Silver → Gold)** using **Docker Compose**, **Redpanda** (a high-performance, C++ Kafka-compatible streaming broker), **PySpark Structured Streaming**, and **Delta Lake**.

**Key Business & Technical Improvements:**
1. **Low-Latency Event Ingestion:** Ingests e-commerce order/clickstream events in near real time into a Bronze landing lakehouse layer.
2. **Event-Time Watermarking & Deduplication:** Uses PySpark Structured Streaming with a **10-minute event-time watermark** to handle late-arriving events and remove duplicate payloads without corrupting analytics.
3. **Automated Lakehouse Compaction:** Implements batch compaction (`OPTIMIZE` / `ZORDER`) in the Gold layer to merge tiny streaming files, ensuring fast, cost-effective downstream query performance.
4. **100% Free & Open-Source Stack:** Runs entirely locally inside Docker without cloud provider usage, credit card requirements, or cloud credit expirations.

---

## 2. Technical Stack & Architecture Diagram

```
+---------------------------------------------------------------------------------------------------+
| LOCAL DOCKER COMPOSE ENVIRONMENT                                                                  |
|                                                                                                   |
|  +-----------------------+         +-----------------------+                                      |
|  | Synthetic Event       |         | Redpanda Broker       |                                      |
|  | Producer (Python)     |  KAFKA  | (Kafka API Compatible)|                                      |
|  | - Emits order events  | ------> |                       |                                      |
|  | - Artificial delays   | PRODUCE | Topic:                |                                      |
|  | - Duplicate event IDs |         | `order-events`        |                                      |
|  +-----------------------+         +-----------------------+                                      |
|                                                |                                                  |
|                                                | KAFKA CONSUME                                    |
|                                                v                                                  |
|  +---------------------------------------------------------------------------------------------+  |
|  | PySpark Structured Streaming Processing Engine                                               |  |
|  |                                                                                             |  |
|  |  +---------------------------------------------------------------------------------------+  |  |
|  |  | BRONZE LAYER: Raw Storage                                                             |  |  |
|  |  | - Streaming Sink: Append-only raw JSON + metadata (Kafka timestamp, partition, offset)|  |  |
|  |  | - Delta Lake Table: `/lakehouse/bronze/order_events`                                  |  |  |
|  |  +---------------------------------------------------------------------------------------+  |  |
|  |                                             |                                               |  |
|  |                                             v                                               |  |
|  |  +---------------------------------------------------------------------------------------+  |  |
|  |  | SILVER LAYER: Cleansed & Deduplicated                                                 |  |  |
|  |  | - Parse JSON fields explicitly                                                        |  |  |
|  |  | - Event-Time Watermarking: 10 minutes (`withWatermark("event_time", "10 minutes")`)    |  |  |
|  |  | - Deduplication: `dropDuplicates(["order_id", "event_time"])`                          |  |  |
|  |  | - Delta Lake Table: `/lakehouse/silver/order_events`                                  |  |  |
|  |  +---------------------------------------------------------------------------------------+  |  |
|  +---------------------------------------------------------------------------------------------+  |
|                                                |                                                  |
|                                                v BATCH SCHEDULED / MANUAL                         |
|  +---------------------------------------------------------------------------------------------+  |
|  | GOLD LAYER: Business Aggregates & File Compaction                                           |  |
|  | - Hourly aggregated sales metrics & revenue per region                                      |  |  |
|  | - File Compaction execution: `OPTIMIZE` & `ZORDER BY (event_date, customer_id)`             |  |  |
|  | - Delta Lake Table: `/lakehouse/gold/hourly_sales`                                         |  |  |
|  +---------------------------------------------------------------------------------------------+  |
+---------------------------------------------------------------------------------------------------+
```

### **Core Stack Components**
* **Containerization:** Docker & Docker Compose
* **Event Broker:** Redpanda (Kafka API-compatible, single-container, zero-JVM footprint)
* **Stream Processing Engine:** PySpark 3.x (Structured Streaming module)
* **Table Format & Storage:** Delta Lake (`delta-spark`), stored on local disk mounts
* **Producer Script:** Python 3.10+ (`confluent-kafka` or `kafka-python`)

---

## 3. Data Schema & Medallion Layer Definitions

### **A. Event Payload Schema (JSON Stream)**
```json
{
  "event_id": "evt_9841a32d-32a2-4a0b-bf88-e9231f28b211",
  "order_id": "ord_10842",
  "customer_id": "cust_5501",
  "seller_id": "seller_201",
  "event_type": "order_created",
  "price": 149.99,
  "freight_value": 12.50,
  "payment_type": "credit_card",
  "event_time": "2026-10-18T10:15:30Z"
}
```

### **B. Medallion Table Architecture**

1. **Bronze Layer (`/lakehouse/bronze/order_events`)**
   * **Format:** Delta Lake
   * **Write Mode:** Append stream
   * **Schema:** `raw_payload` (STRING), `kafka_partition` (INT), `kafka_offset` (LONG), `kafka_timestamp` (TIMESTAMP), `ingest_date` (DATE).

2. **Silver Layer (`/lakehouse/silver/order_events`)**
   * **Format:** Delta Lake
   * **Write Mode:** Append stream with stateful deduplication
   * **Schema:** Explicitly parsed types (`event_id`, `order_id`, `customer_id`, `seller_id`, `event_type`, `price`, `freight_value`, `payment_type`, `event_time`, `total_amount`).
   * **Stateful Logic:** 10-minute watermark on `event_time`, deduped on `order_id` + `event_time`.

3. **Gold Layer (`/lakehouse/gold/hourly_sales`)**
   * **Format:** Delta Lake
   * **Write Mode:** Batch overwrite / append
   * **Grain:** 1 row per hour per seller / payment type.
   * **Optimization:** `OPTIMIZE` execution to merge small micro-batch files into target file sizes, indexed via `ZORDER BY (event_date)`.

---

## 4. Key Engineering Deliverables & Design Patterns

1. **Watermarking & Late Data Strategy:**
   * Distinguish between **Event Time** (when the purchase happened) and **Processing Time** (when PySpark received the event).
   * Demonstrate how state store retention operates under `withWatermark()` to prune old event state and bounded state memory usage.

2. **Fault Tolerance & Checkpointing:**
   * Configure `checkpointLocation` on Delta sinks.
   * Verify **Exactly-Once Semantics** by stopping streaming containers mid-stream (`docker stop`) and restarting to verify no duplicate writes or state corruption.

3. **Small File Problem & Compaction (`OPTIMIZE`):**
   * Explain why streaming micro-batches create small file clutter (e.g., thousands of 50KB files).
   * Implement automated Delta table compaction routines to reorganize partitions into optimized sizes for analytical tools.

4. **Trade-off Analysis Narrative ("Why Streaming is a Batch Problem in Disguise"):**
   * Document in the `README.md` when streaming adds unnecessary operational cost vs. when scheduled micro-batches (e.g., 15-minute scheduled runs) are more appropriate.

---

## 5. Agent Prompt & Execution Requirements

> **Instructions for the AI Assistant / Execution Agent:**
> Act as a Senior Data Engineer and Distributed Systems Architect. You are tasked with generating the starter files and code components for Project 2 (**Streaming Lakehouse with Redpanda, PySpark, and Delta Lake**).

Execute the following file deliverables directly into the user's workspace:

### File Deliverable 1: `docker-compose.yml`
Generate a single, self-contained Docker Compose file provisioning:
* A Redpanda single-node broker (exposing Kafka API on port `9092` and Redpanda Console on port `8081`).
* A PySpark environment with Delta Lake packages pre-installed.

### File Deliverable 2: `producer/event_producer.py`
Create a Python script that continuously streams synthetic order events into the Redpanda topic `order-events`:
* Generates realistic order JSON payloads.
* Introduces **artificial duplicates** (~10% duplicate event IDs).
* Introduces **out-of-order delays** (~10% of events delayed by 2 to 15 minutes).
* Features configurable emission rates (e.g., 50 events/sec) and clean shutdown signals (`SIGINT`/`SIGTERM`).

### File Deliverable 3: `spark/stream_ingestion.py`
Create a PySpark Structured Streaming application that:
1. Connects to the Redpanda Kafka broker (`redpanda:9092`).
2. **Bronze Streaming Sink:** Writes raw ingested messages + Kafka metadata directly to `/lakehouse/bronze/order_events` with checkpointing enabled.
3. **Silver Streaming Processing:** Reads from Bronze stream, parses JSON schema, configures a 10-minute watermark on `event_time`, applies `dropDuplicates(["order_id", "event_time"])`, and streams to `/lakehouse/silver/order_events`.

### File Deliverable 4: `spark/gold_compaction.py`
Create a batch PySpark/Delta script that:
1. Reads from the Silver Delta table `/lakehouse/silver/order_events`.
2. Computes hourly aggregates (`hourly_order_count`, `total_revenue`, `avg_freight_cost`).
3. Writes metrics to `/lakehouse/gold/hourly_sales`.
4. Executes Delta table optimization commands (`OPTIMIZE` / `ZORDER`) on the Gold table to compact small files.

### File Deliverable 5: `README.md`
Generate a complete, interview-ready README structured as a **System Architecture & Design Document**:
* ASCII/Mermaid System Architecture Diagram.
* Table Grain & Medallion Layer Descriptions.
* Explanation of Event-Time Watermarking vs. Processing Time.
* Detailed section titled: *"Why Streaming is a Batch Problem in Disguise" (Trade-offs & Cost Analysis)*.
* Instructions to run, verify streaming, test failure recovery, and inspect Delta tables.

---

## 6. Step-by-Step Roadmap for Weekend Sprint 2

* **Step 1: Container Stack Initialization**
  * Launch Docker Compose stack (`docker compose up -d`).
  * Verify Redpanda health and inspect Redpanda Console (`http://localhost:8081`).
* **Step 2: Stream Event Generation**
  * Execute `event_producer.py` to begin streaming events with synthetic delay and duplication.
* **Step 3: PySpark Streaming Execution**
  * Submit PySpark streaming job (`spark_ingestion.py`).
  * Verify micro-batch generation and state store checkpointing in Bronze and Silver Delta layers.
* **Step 4: Compaction & Fault Tolerance Verification**
  * Execute `gold_compaction.py` to aggregate metrics and compact small parquet files.
  * Stop the streaming container mid-batch, restart it, and assert zero duplicate writes in the Silver Delta table.
* **Step 5: Documentation & Lineage Review**
  * Finalize `README.md` design trade-offs and pin the repository on GitHub.