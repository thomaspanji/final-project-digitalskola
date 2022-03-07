# Data Engineer Final Project

## Scope of Works
This is an individual project in Data Engineer bootcamp given by [DigitalSkola](https://www.digitalskola.com/) to make an end-to-end pipeline to create a data warehouse. All of the process is running on [Google Cloud Platform](https://cloud.google.com/), specifically Google Cloud Storage, Google Composer, Google Dataproc, and Google BigQuery with free tier usage.
![architecure_on_gcp](/img/architecture.png)

The project flow diagram is described in this picture.
![workflow](/img/workflow.png)

Main thing of this project is you process raw files and convert it into Parquet format, save it in the storage, then create external tables for that Parquet files which will be processed further to create data warehouse tables.

As seen in the diagram, we will use the Dataproc cluster on-demand. We create it when we schedule to run Spark application and delete it when execution is finished. Also, we need to create an external PySpark script to convert raw files into Parquet. The rest of our code will reside in an Airflow main script.

## Data
The data provided is in ZIP file. We must extract and upload them into Google Cloud Storage. The data contains eight files, such as immigration data, port, states, airport codes, country, weather, and orders.

## Google Cloud Storage
Google Cloud Storage is used to store our raw files, formatted in CSV. It is also used to store files in staging area which has Parquet format. Apache Airflow from Google Composer also created some default folders to store our dags files, plugins, secret, and data as it installed in a local computer.

## Google Dataproc
Google Dataproc is a managed Spark and Hadoop environment. We must create a cluster to activate the environment. In this project we use an on-demand cluster just for processing a Spark application to convert CSV files to Parquet format. After finishing execution, the cluster will be deleted. Because of free tier usage, the resources given are limited to project and regional quota.

## Google BigQuery
Google BigQuery used to store external tables and create a data warehouse. External table means that we don't put the real data in here buat only create the schema from external source, in this case the Parquet files we created while running Spark application.

We also run queries with table source from the external tables to create a data warehouse. The queries include some casting data types and aggregating data.

## Google Composer
Google Composer is a fully managed workflow orchestration service built on Apache Airflow. Apache Airflow itself is a platform to programmatically author, schedule, and monitor workflows. It is an open-source tool and all built using Python program.

## Apache Airflow

In the main script, we need to import operators that will run some tasks. The operators are for creating Dataproc cluster, running PySpark application, deleting Dataproc cluster, creating external table from Parquet files to BigQuery, and running query in BigQuery itself to make tables of data warehouse. As an addition, there is an operator used to mark the beginning and end of a process. This main script is a Python file.

To be specific, operators are listed in the table below.

| Function  | Operator Name   |
|---|---|
|marking  |DummyOperator   |
|create cluster   |DataprocCreateClusterOperator   |
|delete cluster  |DataprocDeleteClusterOperator   |
|run PySpark  |DataprocSubmitPySparkJobOperator   |
|create external table  |BigQueryCreateExternalTableOperator   |
|run query   |BigQueryInsertJobOperator  |

All definitions and usage details can be seen and learned in [Apache Airflow Github repository](https://github.com/apache/airflow/tree/main/airflow). As we use Google Cloud Platform, operators are mainly located in [here](https://github.com/apache/airflow/tree/main/airflow/providers/google/cloud).

>Operators name mentioned in the table above based on Apache Airflow version 2.x.x. There are many differences when comparing with version 1.x.x, including package name to be called in the script.

From this [script](/dags/dwh_dag.py), we make our graph like the image below.
![airflow-dag](/img/airflow-dag.png)

The duration of this dag to finish execution is about five minutes. Long processes are at creating Dataproc cluster, running PySpark application, and deleting Dataproc cluster.
![task-duration](/img/task-duration.png)

If our resources are sufficient (e.g. memory and CPU availability) and the PySpark application runs heavyweight processing, we can split each task into multiple applications. The graph looks like the following image which produces by [this script](/dags/dwh).
![dag-multicpu](/img/dag-multicpu.png)

## ERD of Data Warehouse Tables

The relationship between tables in the data warehouse we created can be described in ERD, shorts for Entity-Relationship Diagram. We used this [link](https://app.diagrams.net/) to create the diagram.
![erd](/img/erd.png)

## Conclusion

This project implements the Extract, Transform, and Load (ETL) process of creating a data warehouse on the cloud. Skills acquired in this small project are:
- understanding about some Google Cloud Platform products.
- developing ETL pipeline by using Apache Airflow to create and delete a cluster in Google Dataproc, running a PySpark application, creating external tables, and running queries in Google BigQuery.
- reading, processing data, and writing files using Apache Spark to load raw files into staging area.
- defining relation between tables from data warehouse which will be used by the user (e.g. data science and analytics teams).








