# PROCESS.md

## 1. Overview

This project implements the Rearc Data Engineering Quest using Azure Databricks.

The solution ingests two public data sources:

1. U.S. Bureau of Labor Statistics (BLS) productivity time-series files
2. Data USA population API

The data is landed into Databricks Volumes and processed through a Bronze, Silver, and Gold architecture using Spark Declarative Pipelines.

The solution was designed with the following goals:

- Dynamic source ingestion
- Safe and idempotent reruns
- Detection of new, changed, unchanged, and removed source files
- Explicit schemas
- Basic data-quality enforcement
- Reusable PySpark modules
- PySpark and Spark SQL implementations of analytical requirements
- Unity Catalog governance
- Automated orchestration
- Deployment-as-code using Databricks Declarative Automation Bundles

---

## 2. Architecture

The overall data flow is:

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

The complete orchestration is:

```text
Source Ingestion
      |
      v
Bronze Pipeline
      |
      v
Silver Pipeline
      |
      v
Gold Pipeline
```

The four stages are orchestrated through a Databricks Job.

---

## 3. Source Ingestion

### 3.1 BLS Ingestion

The BLS productivity source is:

```text
https://download.bls.gov/pub/time.series/pr/
```

The ingestion process does not hardcode the list of files.

Instead, the source directory is fetched and parsed dynamically to discover available files beginning with:

```text
pr.
```

This allows the ingestion process to respond when the source adds or removes files.

The files are landed under:

```text
/Volumes/adb_rearc_assignment_workspace/bronze/raw_source_files/bls
```

The current source contains files such as:

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

`pr.contacts` and `pr.txt` are retained in the raw layer for completeness and auditability but are not promoted into analytical Delta tables because they are documentation/contact metadata rather than analytical datasets.

### 3.2 BLS 403 Handling

During development, requests to the BLS source initially returned:

```text
HTTP 403 Forbidden
```

The BLS service expects clients to identify themselves appropriately.

The ingestion process therefore supplies a meaningful `User-Agent` containing application/contact information.

This resolved the access issue while following the source-provider usage guidance.

---

## 4. Manifest-Based Incremental Ingestion

A Delta manifest table is used to track ingestion state.

The manifest contains fields such as:

```text
source
file_name
file_path
file_size
source_modified_time
ingestion_time
status
```

The manifest enables the ingestion process to determine whether each source file is:

```text
NEW
CHANGED
UNCHANGED
REMOVED
```

Only NEW or CHANGED files are downloaded and processed.

UNCHANGED files are skipped.

This avoids unnecessary reprocessing and reduces network and compute usage.

### 4.1 New Files

If a file exists in the latest BLS source inventory but does not exist in the latest successful manifest state, it is classified as:

```text
NEW
```

The file is downloaded and recorded in the manifest.

### 4.2 Changed Files

A file is classified as:

```text
CHANGED
```

when its source metadata differs from the last successfully processed version.

The current comparison uses:

- File size
- Source modified timestamp


### 4.3 Unchanged Files

If the source file metadata matches the latest successful manifest record, the file is classified as:

```text
UNCHANGED
```

and is not downloaded again.

This makes repeated executions safe and efficient.

### 4.4 Removed Files

The ingestion process also compares the latest source inventory against the latest manifest state.

If a previously available file no longer exists at the source, it is recorded as:

```text
REMOVED
```

The raw file is not physically deleted from the Databricks Volume.

This is intentional.

Retaining the historical raw file provides auditability and allows historical investigation if required.

---

## 5. Population API Ingestion

Population data is retrieved from the Data USA API.

The raw API response is stored under:

```text
/Volumes/adb_rearc_assignment_workspace/bronze/raw_source_files/population
```

The process compares the newly retrieved response with the existing raw file.

The population source is classified as:

```text
NEW
CHANGED
UNCHANGED
```

If the response is unchanged, the raw file is not rewritten.

This provides simple idempotent behavior for the API source.

---

## 6. Bronze Layer

The Bronze layer represents the raw source structure in Delta tables with minimal transformation.

Important Bronze design decisions:

- Explicit schemas are used instead of schema inference.
- Source column structures are preserved as closely as possible.
- Header names are normalized where required.
- Business-level transformations are intentionally deferred to Silver.

The main Bronze tables include:

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

Using explicit schemas improves predictability and prevents Spark from making unintended type decisions during ingestion.

---

## 7. Silver Layer

The Silver layer performs data cleaning, type conversion, and data-quality enforcement.

The main Silver tables are:

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

This structure separates observations from descriptive metadata and gives the analytical layer reusable fact and dimension tables.

### 7.1 BLS Observation Quality Rules

The BLS observation fact applies validation rules such as:

```text
series_id IS NOT NULL
year IS NOT NULL
period IN ('Q01', 'Q02', 'Q03', 'Q04', 'Q05')
value IS NOT NULL
```

Invalid records are dropped through pipeline expectations.

The source numeric value is converted to a numeric Spark type in Silver.

### 7.2 Population Quality Rules

Population records are filtered to:

```text
Nation = United States
```

Data-quality rules include:

```text
year IS NOT NULL
population > 0
Nation = United States
```

The output contains one population record per year available from the source.

---

## 8. Gold Layer

The Gold layer contains the final analytical datasets requested by the assignment.

Exactly three Gold datasets are produced.

### 8.1 Question 1

#### Requirement

Calculate the mean and standard deviation of annual U.S. population from:

```text
2013 through 2018 inclusive
```

The Gold table is:

```text
gold_q1_population_stats_2013_2018
```

The implementation uses:

```text
AVG(population)
STDDEV_POP(population)
```

`STDDEV_POP` was selected because the calculation is being performed over the complete requested set of annual population observations for the stated period rather than estimating from a sample.

Current result:

```text
mean_population = 322069808
population_stddev = 3796119.936934378
```

### 8.2 Question 2

#### Requirement

For every BLS `series_id`, determine the year having the largest sum of quarterly values.

The Gold table is:

```text
gold_q2_bls_best_year_by_series
```

Only the following periods are included in the yearly sum:

```text
Q01
Q02
Q03
Q04
```

The BLS source also contains:

```text
Q05 = Annual Average
```

Q05 is intentionally excluded because adding the annual average to the four quarterly observations would double-count the annual measurement.

The process is:

```text
BLS observations
      |
      v
Filter Q01-Q04
      |
      v
Group by series_id + year
      |
      v
SUM(value)
      |
      v
Rank descending by annual sum
      |
      v
Select highest-ranked year
```

`DENSE_RANK` is used so ties can be preserved.

A secondary `ROW_NUMBER` ordered by most recent year is also calculated to identify the latest year when tied maximum values exist.

Readable metadata is joined from the BLS dimensions, including fields such as:

- Sector
- Class
- Measure
- Duration
- Seasonal

A combined human-readable series description is also provided.

### 8.3 Question 3

#### Requirement

For:

```text
series_id = PRS30006032
period = Q01
```

return the BLS yearly value together with U.S. population for the corresponding year where population data exists.

The Gold table is:

```text
gold_q3_bls_q01_with_population
```

The join is:

```text
BLS LEFT JOIN Population
        ON BLS.year = Population.year
```

A LEFT JOIN is intentionally used.

This ensures the BLS time series is preserved even for years where population data is unavailable.

Therefore, some rows legitimately contain:

```text
population = NULL
```

---

## 9. PySpark and Spark SQL Implementations

Each analytical requirement was implemented in both:

- PySpark
- Spark SQL

The PySpark implementation is used as the primary Gold pipeline implementation.

The SQL alternatives are stored in:

```text
sql/gold_alternatives.sql
```

The SQL versions were executed independently and compared against the PySpark Gold results.

Parity checks were performed for all three questions.

The SQL and PySpark outputs matched.

---

## 10. Why PySpark Was Selected as the Primary Implementation

PySpark was selected for the primary Gold implementation because the broader solution already uses reusable Python modules and Spark Declarative Pipelines.

This makes it easier to:

- Reuse transformation logic
- Integrate with Python-based testing
- Structure reusable functions
- Maintain one primary transformation language across the pipeline

Spark SQL remains a strong alternative for analytical transformations.

For a real client implementation, the choice between PySpark and Spark SQL would depend on team maintainability, transformation complexity.

---

## 11. Testing

The project includes unit and component-level tests.

Test coverage includes:

- BLS directory parsing
- Source inventory creation
- NEW detection
- CHANGED detection
- UNCHANGED detection
- REMOVED detection
- Configuration loading
- Manifest logic
- Population NEW detection
- Population CHANGED detection
- Population UNCHANGED detection
- Population response validation
- Spark transformation validations

Pure Python tests can be executed independently.

Spark-specific validation was also executed inside Databricks using an active Spark session.

---

## 12. Orchestration

The entire workflow is orchestrated through a Databricks Job:

```text
rearc_data_pipeline_job
```

The dependency graph is:

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

Downstream stages execute only after successful completion of upstream stages.

Retry behavior was also configured for transient failures.

---

## 13. Declarative Automation Bundles

As an additional engineering enhancement, the Databricks Job and pipelines were migrated to Databricks Declarative Automation Bundles.

The repository contains:

```text
databricks.yml
resources/
```

Resource definitions include:

```text
rearc_bronze_pipeline.pipeline.yml
rearc_silver_pipeline.pipeline.yml
rearc_gold_pipelines.pipeline.yml
rearc_data_pipeline_job.job.yml
```

The existing manually created workspace resources were bound to the bundle rather than recreated.

After deployment, the final bundle plan returned:

```text
0 to add
0 to change
0 to delete
4 unchanged
```

The entire Job was then executed successfully through the bundle-managed deployment.

This provides deployment-as-code and makes the Databricks resource configuration version controllable.

---

## 14. Unity Catalog Governance

A read-only analyst group was created:

```text
rearc_gold_analysts
```

The group receives only the permissions required for analytical access to the Gold layer.

The access model is:

```text
USE CATALOG
    |
USE SCHEMA
    |
SELECT
```

The group is not granted write privileges such as:

```text
MODIFY
CREATE TABLE
MANAGE
ALL PRIVILEGES
```


---

## 15. Production Trade-Offs

If this solution were implemented for a real production client, several areas would be strengthened further.

### 15.1 Source Metadata Dependency

The current BLS change-detection logic relies on source-provided metadata such as:

- File size
- Modified timestamp

This avoids downloading every file to calculate a hash.

For a source providing ETags, checksums, version IDs, or stronger metadata, those values would be preferable.

### 15.2 Schema Evolution

The assignment uses explicit expected schemas.

In production, schema drift should be handled according to a defined contract.

Possible strategies include:

- Fail on unexpected schema changes
- Quarantine incompatible records
- Controlled schema evolution
- Source-contract versioning
- Alerting when columns are added, removed, renamed, or change type

Automatic schema evolution should not be enabled blindly for curated layers.

### 15.3 Raw Retention

Removed BLS files are retained rather than deleted.

This is useful for:

- Auditability
- Historical replay
- Troubleshooting

For large-scale production systems, a retention or archival policy would be required to control storage cost.

### 15.4 API Comparison

The population ingestion compares raw response content.

A real production implementation could compare normalized business records or a semantic content hash so harmless formatting differences do not trigger unnecessary processing.

### 15.5 Monitoring and Alerting

A production system would include monitoring for:

- Source availability
- Ingestion failure
- Unexpected source file counts
- Schema changes
- Data-quality failures
- Pipeline duration
- Abnormal record-count changes
- Job failures
- Retry exhaustion

Alerts could be integrated with email, Teams, Slack, PagerDuty, or another operational monitoring platform.

### 15.6 Environment Separation

A real implementation would separate:

```text
DEV
UAT
PROD
```

This separation would include:

- Separate catalogs or schemas
- Separate storage locations
- Environment-specific configuration
- Service principals
- Deployment targets
- Controlled permissions
- Production deployment approvals

Databricks bundle targets could be used to represent these environments.



---

## 16. Challenges and Lessons Learned

### 16.1 BLS Access Restrictions

The initial BLS 403 response required investigation of how the source expected automated clients to identify themselves.

This reinforced the importance of treating upstream public APIs and file servers as external systems with their own usage policies.

### 16.2 Dynamic HTML Source Discovery

The BLS directory is an HTML file listing rather than a structured API manifest.

The ingestion solution therefore had to parse the directory listing dynamically.

This is less robust than consuming a formal API manifest and creates a dependency on the source-page structure.

### 16.3 Correct Manifest State

A key implementation detail was distinguishing between:

```text
latest successful file state
```

and:

```text
latest overall manifest state
```

The latest successful state is useful when comparing source files for NEW or CHANGED detection.

The latest overall state is required for REMOVED detection so already removed files are not repeatedly marked as removed on every run.

### 16.4 BLS Q05 Semantics

The BLS period dimension identifies:

```text
Q05 = Annual Average
```

This is an important business-semantic detail.

Including Q05 in the annual quarterly sum would have produced an incorrect analytical result.

Therefore, Question 2 intentionally uses only Q01-Q04.

### 16.5 Explicit Schema Handling

Initial source parsing exposed issues such as whitespace and non-standard source headers.

Moving to explicit schemas improved stability and made the expected source structure clear.

### 16.6 Testing in Databricks

Pure Python unit tests could be executed independently.

Spark tests required an active Spark context in Databricks, which differs from local Python-only testing.

Separating pure functions from Spark-dependent logic made the project easier to test.

### 16.7 Bundle Migration

The Job and pipelines initially existed as manually configured workspace resources.

Migrating them into Declarative Automation Bundles required:

- Generating bundle resource definitions
- Removing generated duplicate source files
- Correcting repository-relative paths
- Binding existing resources
- Validating deployment plans
- Correcting pipeline root paths so `src` imports worked from the deployed bundle location

After these corrections, the bundle-managed Job executed successfully end to end.

---

## 17. AI Usage

AI assistance was used during development to help with:

- Solution brainstorming
- Code structure suggestions
- Documentation drafting
- Test-case ideas

All suggested code was reviewed, executed, debugged, and validated manually.

The final architecture and code were therefore validated through hands-on execution rather than accepted directly from AI suggestions.

---

## 18. Final Outcome

The final solution provides:

- Dynamic BLS source discovery
- Raw source landing in Databricks Volumes
- Manifest-based incremental ingestion
- Safe reruns
- Detection of source additions, changes, unchanged files, and removals
- Population API ingestion
- Explicit Bronze schemas
- Silver data-quality enforcement
- Three Gold analytical outputs
- Equivalent PySpark and Spark SQL solutions
- Automated pipeline orchestration
- Unit and component tests
- Unity Catalog read-only analyst access
- Databricks Declarative Automation Bundle deployment
- Version-controlled pipeline and Job configuration

The solution was executed end to end successfully in Azure Databricks.
