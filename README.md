# Rearc Data Engineering Quest — Azure Databricks

## Overview

This repository contains an end-to-end Azure Databricks solution for the Rearc Data Engineering Quest.

The project ingests:

- U.S. Bureau of Labor Statistics (BLS) productivity time-series files
- Data USA population API data

The solution uses Databricks Volumes, Spark Declarative Pipelines, Unity Catalog, Bronze/Silver/Gold architecture, Spark SQL, PySpark, Databricks Jobs, and Declarative Automation Bundles.

The implementation focuses on:

- Dynamic source discovery
- Incremental ingestion
- Safe reruns
- Explicit schemas
- Data-quality enforcement
- Reusable PySpark modules
- PySpark and Spark SQL analytical implementations
- Automated orchestration
- Unity Catalog access control
- Deployment-as-code

For detailed implementation decisions, trade-offs, and lessons learned, see [PROCESS.md](PROCESS.md).

---

## Architecture

```text
BLS Website
    |
    v
Dynamic Source Discovery
    |
    v
Raw BLS Files
Databricks Volume
    |
    +-----------------------------+
                                  |
Population API                    |
    |                             |
    v                             |
Raw JSON                          |
Databricks Volume                 |
    |                             |
    +--------------+--------------+
                   |
                   v
              BRONZE LAYER
                   |
                   v
              SILVER LAYER
                   |
                   v
               GOLD LAYER
                   |
                   v
          Analytical Results
```

The full workflow is orchestrated as:

```text
source_ingestion
      |
      v
bronze_pipeline
      |
      v
silver_pipeline
      |
      v
gold_pipeline
```

---

## Repository Structure

```text
Rearc-Assignment-Project/
├── configs/
│   └── config.yaml
├── notebooks/
│   └── 01_source_ingestion
├── pipelines/
│   ├── bronze_pipeline.py
│   ├── silver_pipeline.py
│   └── gold_pipeline.py
├── resources/
│   ├── rearc_bronze_pipeline.pipeline.yml
│   ├── rearc_silver_pipeline.pipeline.yml
│   ├── rearc_gold_pipelines.pipeline.yml
│   └── rearc_data_pipeline_job.job.yml
├── sql/
│   ├── gold_alternatives.sql
│   ├── check_gold_tables.sql
│   └── unity_catalog_grants.sql
├── src/
│   ├── bls_ingestion.py
│   ├── population_ingestion.py
│   ├── manifest.py
│   ├── config_loader.py
│   ├── spark_utils.py
│   └── schemas.py
├── tests/
├── databricks.yml
├── PROCESS.md
├── README.md
└── requirements.txt
```

---

## Data Sources

### BLS Productivity Time Series

Source:

```text
https://download.bls.gov/pub/time.series/pr/
```

The ingestion logic dynamically discovers all available `pr.*` files instead of hardcoding filenames.

Raw files are landed under:

```text
/Volumes/adb_rearc_assignment_workspace/bronze/raw_source_files/bls
```

The current source includes files such as:

```text
pr.series
pr.measure
pr.period
pr.sector
pr.class
pr.duration
pr.seasonal
pr.footnote
pr.contacts
pr.data.0.Current
pr.data.1.AllData
pr.txt
```

`pr.contacts` and `pr.txt` are retained in the raw Volume for completeness but are not promoted into analytical Delta tables.

### Population API

The Data USA API response is stored as raw JSON under:

```text
/Volumes/adb_rearc_assignment_workspace/bronze/raw_source_files/population
```

---

## Incremental Ingestion

A Delta manifest table tracks source state using fields such as:

```text
source
file_name
file_path
file_size
source_modified_time
ingestion_time
status
```

BLS files are classified as:

```text
NEW
CHANGED
UNCHANGED
REMOVED
```

Only NEW and CHANGED files are downloaded and processed.

UNCHANGED files are skipped.

Previously ingested files that disappear from the source are recorded as REMOVED while the raw landed file is retained for auditability.

The population API uses a similar NEW / CHANGED / UNCHANGED decision before rewriting the raw JSON file.

---

## Bronze Layer

Bronze uses explicit schemas and keeps transformations intentionally minimal.

Main tables:

```text
bronze_bls_series
bronze_bls_class
bronze_bls_duration
bronze_bls_footnote
bronze_bls_measure
bronze_bls_period
bronze_bls_seasonal
bronze_bls_sector
bronze_bls_current
bronze_bls_all_data
bronze_population
```

---

## Silver Layer

Silver performs:

- Type conversion
- Standardization
- Data-quality enforcement
- Fact and dimension separation

Main tables:

```text
silver_bls_observations_fact
silver_population_yearly_fact

silver_bls_series_dim
silver_bls_measure_dim
silver_bls_sector_dim
silver_bls_class_dim
silver_bls_duration_dim
silver_bls_seasonal_dim
silver_bls_period_dim
```

Examples of enforced quality rules include:

```text
series_id IS NOT NULL
year IS NOT NULL
period IN ('Q01', 'Q02', 'Q03', 'Q04', 'Q05')
value IS NOT NULL
population > 0
Nation = United States
```

---

## Gold Layer

The Gold layer contains the three required analytical outputs.

### Question 1

Calculate mean and population standard deviation for annual U.S. population from 2013 through 2018 inclusive.

Table:

```text
gold_q1_population_stats_2013_2018
```

Current result:

```text
mean_population = 322069808
population_stddev = 3796119.936934378
```

`STDDEV_POP` is used because the calculation covers the full requested set of annual observations for the period.

### Question 2

For every BLS `series_id`, find the year having the largest sum of quarterly values.

Table:

```text
gold_q2_bls_best_year_by_series
```

Only `Q01` through `Q04` are included.

`Q05` is excluded because the BLS period dimension defines it as:

```text
Q05 = Annual Average
```

Including it in the quarterly sum would double-count the annual measurement.

`DENSE_RANK` is used so ties can be preserved.

### Question 3

For:

```text
series_id = PRS30006032
period = Q01
```

join yearly BLS values with U.S. population where population data is available.

Table:

```text
gold_q3_bls_q01_with_population
```

A LEFT JOIN is used to preserve the BLS time series even when population data is unavailable for a year.

---

## PySpark and Spark SQL

All three analytical requirements are implemented in both:

- PySpark
- Spark SQL

The PySpark implementation is the primary implementation used by the Gold pipeline.

The Spark SQL alternatives are available in:

```text
sql/gold_alternatives.sql
```

The SQL outputs were validated against the PySpark Gold results.

---

## Testing

The repository includes pure Python and Spark-oriented validation coverage.

Tests include:

- BLS directory parsing
- Inventory creation
- NEW detection
- CHANGED detection
- UNCHANGED detection
- REMOVED detection
- Manifest logic
- Configuration loading
- Population change detection
- Population response validation
- Spark transformation checks

Pure Python tests can be run with:

```bash
PYTHONPATH=. PYTHONDONTWRITEBYTECODE=1 python -m pytest -v -p no:cacheprovider
```

Spark-specific validations were executed inside Databricks with an active Spark session.

---

## Orchestration

The complete workflow is orchestrated through:

```text
rearc_data_pipeline_job
```

Task order:

```text
source_ingestion
      |
      v
bronze_pipeline
      |
      v
silver_pipeline
      |
      v
gold_pipeline
```

Downstream tasks run only after upstream success.

Retry behavior is configured for transient failures.

---

## Declarative Automation Bundles

The Databricks Job and all three Spark Declarative Pipelines are managed through Databricks Declarative Automation Bundles.

Bundle files include:

```text
databricks.yml
resources/rearc_bronze_pipeline.pipeline.yml
resources/rearc_silver_pipeline.pipeline.yml
resources/rearc_gold_pipelines.pipeline.yml
resources/rearc_data_pipeline_job.job.yml
```

The existing workspace resources were bound to the bundle instead of recreated.

After deployment, the final plan returned:

```text
0 to add
0 to change
0 to delete
4 unchanged
```

The bundle-managed end-to-end Job was then executed successfully.

Useful commands:

```bash
databricks bundle validate
databricks bundle plan
databricks bundle deploy
databricks bundle run rearc_data_pipeline_job
```

---

## Unity Catalog Governance

A read-only analyst group was created:

```text
rearc_gold_analysts
```

The intended Gold-layer access model is:

```text
USE CATALOG
USE SCHEMA
SELECT
```

Example grants are stored in:

```text
sql/unity_catalog_grants.sql
```

The analyst role is intentionally not granted write privileges such as `MODIFY`, `CREATE TABLE`, `MANAGE`, or `ALL PRIVILEGES`.

---

## Configuration

Environment and source settings are stored in:

```text
configs/config.yaml
```

The configuration includes:

- Catalog
- Schemas
- Volume paths
- BLS source URL
- Population API URL
- Source-specific target paths
- Manifest configuration

For a real production deployment, environment-specific configuration and secrets should be externalized and managed through approved secret-management mechanisms.

---

## Running the Project

Recommended execution path:

### 1. Validate the bundle

```bash
databricks bundle validate
```

### 2. Review planned changes

```bash
databricks bundle plan
```

### 3. Deploy

```bash
databricks bundle deploy
```

### 4. Run the full workflow

```bash
databricks bundle run rearc_data_pipeline_job
```

The same Job can also be executed from the Databricks Jobs UI.

---

## Key Design Decisions

- BLS filenames are discovered dynamically.
- Incremental ingestion uses manifest metadata instead of re-downloading every BLS file for hashing.
- Removed source files are recorded but raw files are retained.
- Explicit schemas are used in Bronze.
- Business transformations are deferred to Silver.
- PySpark is the primary Gold implementation.
- Spark SQL alternatives are retained for comparison and review.
- `Q05` is excluded from quarterly aggregation because it represents Annual Average.
- LEFT JOIN is used in Question 3 to preserve the BLS series.
- Existing Databricks resources were bound to the bundle instead of duplicated.
- Gold access is separated from engineering access through Unity Catalog permissions.

Detailed rationale and production trade-offs are documented in [PROCESS.md](PROCESS.md).

---

## AI Usage

AI assistance was used for brainstorming, code-structure suggestions, documentation drafting, test-case ideas.

All suggested code and design decisions were reviewed, executed, debugged, and validated manually before being included in the final solution.

See [PROCESS.md](PROCESS.md) for more detail.

---

## Final Status

The solution has been executed successfully end to end in Azure Databricks.

Implemented components include:

- Dynamic BLS ingestion
- Population API ingestion
- Manifest-based source tracking
- Bronze / Silver / Gold pipelines
- Data-quality expectations
- Three required Gold outputs
- PySpark and Spark SQL implementations
- Testing
- Databricks Job orchestration
- Unity Catalog read-only analyst access
- Declarative Automation Bundle deployment
- Version-controlled Job and pipeline definitions
