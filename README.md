# 🚀 E-Commerce Big Data Pipeline

A complete end-to-end **Big Data Engineering** pipeline that processes e-commerce data using industry-standard tools. The pipeline automates data ingestion, transformation, and visualization — all running in Docker containers.

![Python](https://img.shields.io/badge/Python-3.11-blue?logo=python)
![Spark](https://img.shields.io/badge/Apache%20Spark-3.5.1-orange?logo=apachespark)
![Airflow](https://img.shields.io/badge/Apache%20Airflow-2.9.1-green?logo=apacheairflow)
![HDFS](https://img.shields.io/badge/HDFS-3.2.1-yellow?logo=apachehadoop)
![Docker](https://img.shields.io/badge/Docker-Compose-blue?logo=docker)

---

## 📋 Table of Contents

- [Overview](#-overview)
- [Architecture](#-architecture)
- [Tech Stack](#-tech-stack)
- [Project Structure](#-project-structure)
- [Dataset](#-dataset)
- [Pipeline Stages](#-pipeline-stages)
- [Dashboard](#-dashboard)
- [Prerequisites](#-prerequisites)
- [Installation & Setup](#-installation--setup)
- [Usage](#-usage)
- [Web Interfaces](#-web-interfaces)
- [Troubleshooting](#-troubleshooting)

---

## 🔍 Overview

This project builds a **fully automated data pipeline** for an e-commerce platform. It takes raw CSV data (users, orders, products, reviews, events), stores it in a distributed file system (HDFS), processes and transforms it using Apache Spark, orchestrates the entire workflow with Apache Airflow, and visualizes the results in an interactive Streamlit dashboard.

### What This Pipeline Does:

1. **Ingests** raw CSV files into HDFS (Hadoop Distributed File System)
2. **Cleans & Transforms** the data using PySpark (removes duplicates, fixes types, joins tables)
3. **Aggregates** business metrics (monthly sales, top products, customer segments, etc.)
4. **Saves** processed data as optimized Parquet files back to HDFS
5. **Orchestrates** the entire workflow automatically via an Airflow DAG
6. **Visualizes** the results in a real-time interactive dashboard

---

## 🏗 Architecture

```
┌──────────────────────────────────────────────────────────────────┐
│                        Docker Environment                        │
│                                                                  │
│  ┌─────────┐    ┌──────────────┐    ┌──────────────────────┐    │
│  │  CSV     │───▶│    HDFS      │───▶│    Apache Spark      │    │
│  │  Files   │    │  (NameNode + │    │  (Master + Worker)   │    │
│  │  (data/) │    │   DataNode)  │◀───│  ETL Processing      │    │
│  └─────────┘    └──────────────┘    └──────────────────────┘    │
│       │                                        │                 │
│       │              ┌─────────────┐           │                 │
│       └─────────────▶│  Airflow    │───────────┘                 │
│                      │  (Scheduler │                             │
│                      │  + WebUI)   │                             │
│                      └─────────────┘                             │
│                                                                  │
│  ┌──────────────┐    ┌──────────────┐                           │
│  │  PostgreSQL   │    │  Streamlit   │                           │
│  │  (Metadata)   │    │  Dashboard   │                           │
│  └──────────────┘    └──────────────┘                           │
└──────────────────────────────────────────────────────────────────┘
```

---

## 🛠 Tech Stack

| Technology | Version | Purpose |
|-----------|---------|---------|
| **Apache Spark** | 3.5.1 | Distributed data processing (ETL) |
| **Apache Airflow** | 2.9.1 | Workflow orchestration & scheduling |
| **HDFS (Hadoop)** | 3.2.1 | Distributed file storage |
| **PostgreSQL** | 15 | Airflow metadata database |
| **Streamlit** | 1.37.0 | Interactive dashboard |
| **Plotly** | 5.22.0 | Data visualization charts |
| **Docker** | Compose | Container orchestration |
| **Python** | 3.11 | Programming language |

---

## 📁 Project Structure

```
├── docker-compose.yml        # Orchestrates all containers
├── Dockerfile.spark          # Spark image with Python support
├── Dockerfile.airflow        # Airflow image with Java + Spark
├── Dockerfile.dashboard      # Streamlit dashboard image
├── hadoop.env                # HDFS configuration
├── requirements.txt          # Python dependencies for Airflow
├── .gitignore                # Git ignore rules
├── README.md                 # This file
│
├── data/                     # Raw CSV dataset (not in git)
│   ├── users.csv
│   ├── products.csv
│   ├── orders.csv
│   ├── order_items.csv
│   ├── reviews.csv
│   └── events.csv
│
├── spark-apps/               # Spark ETL jobs
│   └── etl_job.py            # Main ETL pipeline script
│
├── dags/                     # Airflow DAGs
│   └── ecommerce_dag.py      # Pipeline orchestration DAG
│
├── dashboard/                # Streamlit dashboard
│   └── app.py                # Dashboard with charts & KPIs
│
└── logs/                     # Airflow logs (auto-generated)
```

---

## 📊 Dataset

The pipeline processes an **E-Commerce dataset** with 6 related CSV files:

| File | Records | Description |
|------|---------|-------------|
| `users.csv` | 10,000 | Customer profiles (name, email, gender, city) |
| `products.csv` | 2,000 | Product catalog (name, category, price, rating) |
| `orders.csv` | 20,000 | Order transactions (date, status, total amount) |
| `order_items.csv` | 43,525 | Individual items in each order (quantity, price) |
| `reviews.csv` | 15,000 | Product reviews (rating, text, date) |
| `events.csv` | 80,000 | User behavior events (view, cart, purchase) |

> **Dataset Source:** [E-Commerce Dataset on Kaggle](https://www.kaggle.com/datasets/abhayayare/e-commerce-dataset) — Download and place the CSV files in the `data/` folder before running the pipeline.

---

## ⚙️ Pipeline Stages

### Stage 1: Data Ingestion (HDFS)
- Raw CSV files are uploaded from local storage to HDFS `/data/raw/`
- Creates a distributed, fault-tolerant storage layer

### Stage 2: ETL Processing (Spark)
The Spark job (`spark-apps/etl_job.py`) performs:

| Step | Operation | Details |
|------|-----------|---------|
| **Extract** | Read CSVs | Loads all 6 files from HDFS |
| **Clean** | Remove duplicates | De-duplicates by primary keys |
| **Clean** | Handle nulls | Drops rows missing critical fields |
| **Clean** | Fix types | Casts columns to proper data types |
| **Clean** | Standardize | Trims whitespace, normalizes case |
| **Transform** | Join tables | Creates full order details (4-table join) |
| **Transform** | Add columns | Extracts year, month; calculates totals |
| **Aggregate** | Monthly sales | Revenue trends over time |
| **Aggregate** | Category sales | Revenue by product category |
| **Aggregate** | Top products | Best-selling products |
| **Aggregate** | Customer summary | Spending per customer |
| **Aggregate** | Order status | Completed vs cancelled vs returned |
| **Aggregate** | Event funnel | View → Cart → Purchase conversion |
| **Aggregate** | City sales | Revenue by city |
| **Load** | Save Parquet | Writes 10 datasets to HDFS `/data/processed/` |

### Stage 3: Orchestration (Airflow)
The Airflow DAG (`dags/ecommerce_dag.py`) automates the pipeline with 3 tasks:

```
Task 1: Create HDFS Directories  →  Task 2: Upload CSVs to HDFS  →  Task 3: Run Spark ETL
```

- **Task 1** creates `/data/raw` and `/data/processed` directories in HDFS
- **Task 2** uploads all 6 CSV files to HDFS using WebHDFS API
- **Task 3** submits the Spark ETL job to the Spark cluster

### Stage 4: Visualization (Dashboard)
Interactive Streamlit dashboard reads processed Parquet data from HDFS and displays real-time visualizations.

---

## 📈 Dashboard

The Streamlit dashboard provides 8 visualizations:

| # | Visualization | Chart Type |
|---|--------------|------------|
| 1 | KPI Cards (Revenue, Orders, Customers, Avg Value) | Metric cards |
| 2 | Monthly Sales Trend | Line chart |
| 3 | Sales by Category | Horizontal bar chart |
| 4 | Order Status Breakdown | Donut chart |
| 5 | Top 10 Products | Horizontal bar chart |
| 6 | Conversion Funnel (View → Cart → Purchase) | Funnel chart |
| 7 | Top 10 Cities by Revenue | Horizontal bar chart |
| 8 | Top 20 Customers | Interactive table |

---

## 📦 Prerequisites

Before you begin, make sure you have:

- **Docker** (v20+) and **Docker Compose** (v2+) installed
- **Git** installed
- At least **8 GB RAM** available for Docker
- At least **10 GB free disk space**

### Install Docker on Fedora/RHEL:
```bash
sudo dnf install docker docker-compose-plugin
sudo systemctl start docker
sudo systemctl enable docker
sudo usermod -aG docker $USER
# Log out and log back in for group changes to take effect
```

### Install Docker on Ubuntu/Debian:
```bash
sudo apt update
sudo apt install docker.io docker-compose-v2
sudo systemctl start docker
sudo usermod -aG docker $USER
```

---

## 🚀 Installation & Setup

### Step 1: Clone the Repository

```bash
git clone https://github.com/AhmedElsenosy/ecommerce-data-pipeline.git
cd ecommerce-data-pipeline
```

### Step 2: Add the Dataset

Download the [E-Commerce Dataset from Kaggle](https://www.kaggle.com/datasets/abhayayare/e-commerce-dataset) and place the 6 CSV files in the `data/` folder:

```
data/
├── users.csv
├── products.csv
├── orders.csv
├── order_items.csv
├── reviews.csv
└── events.csv
```

### Step 3: Create Required Directories

```bash
mkdir -p logs plugins
chmod -R 777 logs
```

### Step 4: Build and Start All Containers

```bash
docker compose up -d --build
```

This starts **8 containers**:
| Container | Service |
|-----------|---------|
| `namenode` | HDFS NameNode |
| `datanode` | HDFS DataNode |
| `spark-master` | Spark Master |
| `spark-worker` | Spark Worker |
| `postgres` | PostgreSQL (Airflow metadata) |
| `airflow-webserver` | Airflow Web UI |
| `airflow-scheduler` | Airflow Scheduler |
| `dashboard` | Streamlit Dashboard |

> Wait about **60 seconds** for all services to fully initialize.

### Step 5: Verify All Containers Are Running

```bash
docker compose ps
```

All containers should show `Up` or `Running` status.

### Step 6: Verify HDFS Is Healthy

```bash
docker exec namenode hdfs dfsadmin -report
```

Look for `Live datanodes (1)` in the output.

### Step 7: Set HDFS Permissions

```bash
docker exec namenode hdfs dfs -mkdir -p /data/raw /data/processed
docker exec namenode hdfs dfs -chmod -R 777 /data
```

---

## ▶️ Usage

### Option A: Run via Airflow UI (Recommended)

1. Open Airflow UI at **http://localhost:8082**
2. Login with username: **admin** / password: **admin**
3. Find the **`ecommerce_etl_pipeline`** DAG
4. Toggle it **ON** (switch on the left)
5. Click **▶ Trigger DAG** (play button on the top right)
6. Watch the 3 tasks turn green as they complete ✅

### Option B: Run Manually via Terminal

**Upload data to HDFS:**
```bash
docker exec namenode hdfs dfs -put /data/events.csv /data/raw/
docker exec namenode hdfs dfs -put /data/order_items.csv /data/raw/
docker exec namenode hdfs dfs -put /data/orders.csv /data/raw/
docker exec namenode hdfs dfs -put /data/products.csv /data/raw/
docker exec namenode hdfs dfs -put /data/reviews.csv /data/raw/
docker exec namenode hdfs dfs -put /data/users.csv /data/raw/
```

**Run the Spark ETL job:**
```bash
docker exec spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  /opt/spark-apps/etl_job.py
```

**Verify processed data:**
```bash
docker exec namenode hdfs dfs -ls /data/processed/
```

### View the Dashboard

Open **http://localhost:8501** to see the interactive analytics dashboard.

---

## 🌐 Web Interfaces

| Service | URL | Credentials |
|---------|-----|-------------|
| **Airflow UI** | http://localhost:8082 | admin / admin |
| **Streamlit Dashboard** | http://localhost:8501 | — |
| **Spark Master UI** | http://localhost:8080 | — |
| **Spark Worker UI** | http://localhost:8081 | — |
| **HDFS NameNode UI** | http://localhost:9870 | — |
| **HDFS DataNode UI** | http://localhost:9864 | — |

---

## 🔧 Troubleshooting

### Containers not starting?
```bash
docker compose down -v
docker compose up -d --build
```

### HDFS DataNode not connecting?
Check the datanode logs:
```bash
docker compose logs datanode
```
Make sure `hadoop.env` uses `CORE_CONF_` prefix (not `CORE_SITE_`).

### Permission denied on HDFS?
```bash
docker exec namenode hdfs dfs -chmod -R 777 /data
```

### Airflow DAG not appearing?
Wait 30 seconds for Airflow to scan the `dags/` folder. Check logs:
```bash
docker compose logs airflow-scheduler
```

### Dashboard shows no data?
Make sure the ETL pipeline has run successfully first (via Airflow or manually).

---

## 🛑 Stop the Pipeline

```bash
docker compose down
```

To stop and **remove all data volumes**:
```bash
docker compose down -v
```

---

## 📄 License

This project is open source and available under the [MIT License](LICENSE).

---

<p align="center">
  Built with ❤️ using Spark + HDFS + Airflow + Streamlit
</p>
