# final-project-digitalskola

This is an individual project in Data Engineer bootcamp given by [DigitalSkola](https://www.digitalskola.com/) to make an end-to-end pipeline to create data warehouse. All of the process is running on [Google Cloud Platform](https://cloud.google.com/), specifically Google Cloud Storage, Google Composer, Google Dataproc, and Google BigQuery.

As it is called, Google Cloud Storage (GCS) is used to store our data, such as raw files and staging files. Google Composer is a fully-managed Apache Airflow. Google Dataproc is used to host Apache Spark and all Hadoop enviroments. At last, Google BigQuery is used to store our data warehouse tables. Here is the archicture for the project.

![architecure_on_gcp](/src/architecture.png)

In this repository, I am not going to explain step-by-step initializing the project. I will focus about authoring workflow and scheduling it to run automatically. However, I will explain the architecture I used in this project on another repository soon.

The workflow that I will create is described in this picture.
![workflow](/src/workflow.png)

Main thing on the workflow is you process raw files and convert it into Parquet format, save it in the storage, then create external tables which will be processed further to create data warehouse.

After the architecture has been set up, first task you must do is creating bucket to store input files and processed or staging files. You can create it using [Console](https://cloud.google.com/storage/docs/creating-buckets#console) which accesses the web service or via [gcloud CLI](https://cloud.google.com/storage/docs/creating-buckets#gsutil).

By using CLI, to a create bucket you need to write following command.

```
gsutil mb \
-p final-project-de \
-c standard -l us-central1 \
-b on gs://final_project_de`
```

`gsutil mb` is command to make bucket. `-p final-project-de` is the name of the project you've created before. `-c standard` is define storage class. `-l us-central1` is the location of the bucket will be placed to. Last, `-b on gs://gs://final_project_de` means create bucket with name 'gs://final_project_de`. More explanation go to this [link](www.google.com).

To add local files to the bucket, you simply put all the files into a folder, mine is `input` folder. Move to parent folder of `input`, then type this command,

`gsutil cp -r input gs://final_project_de`

This will create files within the bucket with prefix `input/`. For example, the actual URI of `your_file.txt` will be `gs://final_project_de/input/your_files.txt`.

> Google Cloud Storage doesn't define folder inside a bucket. Instead, its uses prefix as an abstraction for folder name. More on [this](https://cloud.google.com/storage/docs/folders).

The second task is creating dataset inside Google BigQuery. This also can be done using [many ways](https://cloud.google.com/bigquery/docs/datasets#create-dataset). By using CLI, type this command

```
bq --location=US mk -d \
--default_table_expiration 3600 \
--description "Staging dataset for creating data warehouse." \
final_project_staging
```
and
```
bq --location=US mk -d \
--default_table_expiration 3600 \
--description "Dataset for data warehouse." \
final_project_dwh
```

`bq` is a command to utilize BigQuery functionality. I created dataset with name `final_project_staging` and `final_project_dwh`. The argument `--default_table_expiration` is used to set expiry date of tables inside a dataset in seconds.

All is set, so we move to the main part of this project. Yes, coding time!

We need a configuration file to be called when Airflow runs. Actually, we can hard-coded every configuration needed in the main Airflow file, but it is not the best practice when we are coding our program. You will see why. This configuration file will be imported as a JSON file in Airflow variables.

The main key of the JSON file is a [DAG](https://airflow.apache.org/docs/apache-airflow/1.10.12/concepts.html#dags) name and the value is the variables which correspond to certain products. Here, I make key-value pairs for general info, Dataproc cluster (`dataproc`), BigQuery (`bq`), and Cloud Storage (`gcs`).










