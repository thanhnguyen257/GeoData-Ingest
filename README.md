# GeoData-Ingest
This project is a simplified re-implementation of the paper  
[*Efficient IoT Data Management for Geological Disasters Based on Big Data-Turbocharged Data Lake Architecture* (Huang et al., 2021)](https://doi.org/10.3390/ijgi10110743).  

It focuses on experimenting with data engineering techniques for ingesting and processing open geo-meteorological data, using limited hardware and smaller datasets.  

---

## 🚀 Features

- **Data Ingestion**
  - Fetch geo/meteorological data from public APIs (e.g., Shenzhen Open Data).  
  - Store ingested data into a lightweight Ceph cluster (“micro-Ceph”).  
  - Multithreaded ingestion pipeline:
    - Task abstraction for API requests.  
    - Queue + thread pool for parallel execution.  
    - Custom tasks for different API structures.  

- **Data Storage & Processing**
  - Compare **JSON vs. Parquet** formats using Apache Spark.  
  - Benchmark **partitioned vs. non-partitioned** Parquet storage.  
  - Measure ingestion time across different **thread counts**.  

- **Metadata & Visualization**
  - Extract metadata from ingested datasets.  
  - Store metadata in **Elasticsearch**.  
  - Visualize dataset info and metrics in **Kibana** dashboards.  

---

## ⚖️ Comparison with the Paper

| Aspect | Original Paper | This Project |
|--------|----------------|--------------|
| Data volume | Large-scale IoT & geohazard data | Limited open datasets (geo/meteo APIs) |
| Storage | Distributed Ceph cluster | Small “micro-Ceph” |
| Ingestion | Multi-threaded, multi-node | Multi-threaded, single-node |
| Processing | Spark + Ceph + Alluxio cache | Spark only |
| Metadata | ISO-based, Elasticsearch | Simplified, Elasticsearch |
| Goal | Full data lake for geohazards | Demo + benchmarking for learning |

---

## 📊 Example Experiments

- Time comparison: JSON vs Parquet.  
- Query performance: partitioned vs non-partitioned Parquet.  
- Ingestion speed: varying number of threads (1 → N).  
- Metadata search & visualization in Kibana.  

---

## 🔧 Requirements

- Python 3.x  
- Apache Spark  
- Ceph (single node or small cluster)  
- Elasticsearch + Kibana  

---

## 📌 Notes

- This is a **demo project** — results are limited by hardware and dataset size.  
- Focus is on showcasing **data engineering skills** and experimenting with ingestion/processing pipelines.
