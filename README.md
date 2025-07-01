# High Performance Computing (HPC) Operational Data Analytics (ODA) Database (DB) Benchmarking

> Benchmarking previously and currently implemented ODA DBs utilized at super
> computing facilities on simultainous data ingress and egress

## Table of Contents

- [High Performance Computing (HPC) Operational Data Analytics (ODA) Database (DB) Benchmarking](#high-performance-computing-hpc-operational-data-analytics-oda-database-db-benchmarking)
  - [Table of Contents](#table-of-contents)
  - [Abstract](#abstract)
  - [Intellectual Merit](#intellectual-merit)
  - [Broader Impacts](#broader-impacts)
  - [Databases Benchmarked](#databases-benchmarked)
    - [IBM DB2 Community Edition Notes](#ibm-db2-community-edition-notes)
      - [Acquiring the JDBC driver](#acquiring-the-jdbc-driver)

## Abstract

Super computers are heterogenous systems composed of many compute resources that
allow users to distribute and parallelize workloads. These systems are
expensive, and monitoring them allows business intelligence, maintence, and
systems research teams to make business, operational, and organization decsions
that optimize the return on investment of these systems. To do so, system and
environmental metrics are captured by stakeholders in real time and stored in
databases and lakehouses on premises to retain data security and integrity.

However, with the rise of exascale super computing, the amount of data captured
is approaching *petabytes per day*. The scale of this data strains existing
database solutions at premier super computing facilities, with one anecdote
claiming that querying the database while writing data to it consumes ~70% of
CPU resources on the distributed database solution. As super computing resources
continue to grow it is expected that the simultainous ingress and egress of data
on database solutions will require significantly more computational resources to
enable real time ODA, including heuristics, visualizations, and machine learning
operations. This repository is a test suite of the scalability and performance
of simultainous ODA operations on databases. The test data is from publicly
availible *environmental data* from super computing facilities.

## Intellectual Merit

> TODO: Add me!

## Broader Impacts

> TODO: Add me!

## Databases Benchmarked

| **Database**                  | **Open Source** | **Storage Format** |
| ----------------------------- | --------------- | ------------------ |
| IBM DB2 BLU Community Edition | No              | Columnar           |
| Apache Druid                  | Yes             | Columnar           |
| Apache Pinot                  | Yes             | Columnar           |
| ClickHouse                    | Yes             | Columnar           |
| Apache HBase                  | Yes             | Column-Oriented    |
| MySQL Community Edition       | Yes             | Row                |
| Postgres                      | Yes             | Row                |
| InfluxDB                      | Yes             | Time series        |

### IBM DB2 Community Edition Notes

#### Acquiring the JDBC driver

You can find the download for the JDBC driver used in this work on
[this webpage](https://www.ibm.com/support/fixcentral/swg/selectFixes?source=dbluesearch&product=ibm%2FInformation+Management%2FIBM+Data+Server+Client+Packages&release=12.1.2.0&platform=Linux+32-bit,x86&searchtype=fix&fixids=*jdbc*FP000&function=fixId&parent=ibm/Information%20Management).
You do not need to download any of the prerequisite packages. Rename the JDBC
driver from `db2jcc.jar` to `db2.jar` after extracting it.
