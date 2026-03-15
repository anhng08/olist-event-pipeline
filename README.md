# Olist E-commerce Event Pipeline

A dual-layer data pipeline that processes and tracks e-commerce events from the [Olist Brazilian E-commerce dataset](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce), stores them in MongoDB, and visualizes insights via Apache Superset.

## Architecture
```
┌─────────────────────────────────────────────────────────────┐
│                     DATA SOURCE                              │
│              Olist CSV (100k+ orders, 5 tables)             │
└───────────────────┬─────────────────────────────────────────┘
                    │
        ┌───────────┴───────────┐
             ▼                                      ▼
┌──────────────────┐   ┌─────────────────────┐
│   BATCH LAYER    │   │    STREAM LAYER      │
│                  │   │                      │
│  PySpark cluster │   │  Kafka (3 partitions)│
│  - Extract       │   │  - Producer          │
│  - Transform     │   │  - Consumer          │
│  - Load          │   │                      │
└────────┬─────────┘   └──────────┬───────────┘
         │                        │
         └───────────┬────────────┘
                                   ▼
         ┌───────────────────────┐
         │       MongoDB         │
         │   3-node Replica Set  │
         │   (PRIMARY + 2x SEC.) │
         │   488,254 events      │
         └───────────┬───────────┘
                     │
                                   ▼
         ┌───────────────────────┐
         │      PostgreSQL       │
         │   (Analytical Store)  │
         │   fct_events          │
         │   fct_reviews         │
         │   fct_order_items     │
         └───────────┬───────────┘
                     │
                                   ▼
         ┌───────────────────────┐
         │    Apache Superset    │
         │   Interactive Dashboard│
         │   6 charts + 4 KPIs   │
         └───────────────────────┘
```

## Design Decisions

### Why MongoDB?
- **Event-driven schema**: Each event has a different payload structure (order events carry `items` array, review events carry `review_score`). MongoDB's flexible document model handles this naturally without schema migrations.
- **Fault tolerance**: 3-node replica set ensures data availability even if 1 node goes down. Writes are acknowledged by PRIMARY, replicated asynchronously to 2 SECONDARY nodes.
- **Operational store**: MongoDB serves as the raw event store — every event is persisted as-is with full payload, enabling replay and reprocessing.

### Why Apache Spark?
- **Scalable joins**: Joining 5 CSV tables (100k+ rows each) and exploding timestamps into 488k+ events benefits from Spark's distributed execution across 2 workers.
- **Learning path**: While pandas could handle this dataset size, Spark's API (`collect_list`, `struct`, `foreachPartition`) maps directly to production-scale patterns used at companies processing billions of events.
- **Cluster mode**: Running on a real Spark cluster (master + 2 workers) via Docker demonstrates understanding of distributed compute, not just local scripts.

### Why Kafka?
- **Decoupling**: Producer and consumer are fully independent — producer can emit events at any rate, consumer processes at its own pace without data loss.
- **Fault tolerance**: `enable_auto_commit=False` with manual commit means if the consumer crashes mid-batch, it replays from the last committed offset — no events lost.
- **Real-time simulation**: Events are emitted in chronological order based on original Olist timestamps, accurately simulating a real-time e-commerce event stream.

### Why PostgreSQL as Serving Layer?
- MongoDB is optimized for writes and document retrieval, not analytical GROUP BY queries across 400k+ documents.
- PostgreSQL with proper indexes serves Superset queries in milliseconds vs seconds on MongoDB.
- Clean separation: MongoDB = operational store, PostgreSQL = analytical store. This mirrors the Lambda Architecture pattern.

## Pipeline Stats

| Layer | Count |
|-------|-------|
| Raw orders | 99,441 |
| Batch events generated | 488,254 |
| Stream events processed | 392,856 |
| MongoDB documents | 488,254 |
| PostgreSQL fct_events | ~392k |
| PostgreSQL fct_order_items | 112,650 |

## Event Types

| Event | Description |
|-------|-------------|
| `order_placed` | Customer submitted an order |
| `order_approved` | Payment approved |
| `order_shipped` | Handed to carrier |
| `order_delivered` | Delivered to customer |
| `review_submitted` | Customer left a review |

## Dashboard

- **Total Revenue**: R$ ~13.6M
- **Average Order Value**: R$ ~137
- **Total States**: 27
- **Total Orders**: 99,441
- **Top Category**: beleza_saude (beauty & health)
- **Peak Week**: Black Friday 2017 (~3,000 orders)
