# Enhancing ETL Processes with DBT and Trino: A Modern Approach to Data Transformation
 DBT (Data Build Tool) and Trino offer a powerful combination for managing ETL (Extract, Transform, Load) processes within modern data architectures. DBT specializes in transforming data that's been loaded into your warehouse, functioning as a transformation layer that enables data analysts and engineers to write modular SQL queries, which it runs against your data warehouse. It helps define data models, test data quality, and document the entire data transformation process, making the workflow highly efficient and scalable.

Trino, formerly known as PrestoSQL, is an open-source distributed SQL query engine designed for running interactive analytic queries against data sources of all sizes ranging from gigabytes to petabytes. It acts as a virtual data warehouse and allows querying data where it lives, including structured and unstructured sources, without requiring data movement. By integrating DBT with Trino, teams can leverage Trino’s ability to perform distributed SQL queries across various sources for initial data extraction and loading, followed by transformation tasks managed through DBT. This integration effectively harnesses the strengths of both tools—Trino's fast data querying capabilities and DBT’s robust data modeling and transformation features—to streamline the ETL process, enhance data integrity, and accelerate time-to-insight in analytics projects.

## setup and config
https://docs.getdbt.com/docs/core/connect-data-platform/trino-setup
## staging
- Incremental
- Source of Image
## raw_vault
- hub
- link
- satellite
