import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import year
from pyspark.sql.functions import avg
from google.cloud import storage


spark = SparkSession.builder \
    .appName("Convert to Parquet") \
    .getOrCreate()

# spark.conf.set("google.cloud.auth.service.account.enable", "true")


# Create function for reading csv files
def read(filename,sep=None):
    """ Function to read CSV file.
    
    CSV file located in Google Cloud Storage with prefix 'input/' acted as 
    raw file. The output is a Spark dataframe.
    """

    if sep is None:
        df = spark.read \
                .option('header', 'true') \
                .option('inferSchema', 'true') \
                .csv(f'gs://final_project_de/input/{filename}.csv')
        
        df.printSchema()

    else:
        df = spark.read \
        .option('sep', sep) \
        .option('header', 'true') \
        .option('inferSchema', 'true') \
        .csv(f'gs://final_project_de/input/{filename}.csv')

        df.printSchema()

    return df


# Create function to write file into parquet format
def write(filename, df=None, partition=None):
    """ Function to write dataframe into Parquet file.
    
    The Parquet file is saved into Google Cloud Storage as staging phase with 
    added prefix 'staging/'.
    """
    
    filename = filename.replace('-', '_').lower()
    
    if df is None:
        df_write = read(filename)
    df_write = df

    print('Starting ...')

    if partition is None:
        df_write.write \
            .mode('overwrite') \
            .parquet(f'gs://final_project_de/staging/{filename}.parquet')
    else:
        df_write.write \
            .partitionBy(partition) \
            .mode('overwrite') \
            .parquet(f'gs://final_project_de/staging/{filename}.parquet')


# Create client to access bucket in Google Cloud Storage
client = storage.Client(project='final-project-de')
blobs = client.get_bucket('final_project_de').list_blobs(prefix='input/')
files = [file.name for file in list(blobs)]


for file in files:

    filename = file.replace('input/', '').replace('.csv','')

    if filename == 'GlobalLandTemperaturesByCity':
        print(f'Reading file {filename}.csv ....')

        df = read(filename)
        df_year = df.withColumn('year', year('dt')) \
            .select('year','AverageTemperature','City','Country','Latitude','Longitude') \
            .groupBy('year','City','Country','Latitude','Longitude') \
            .agg(avg('AverageTemperature').alias('AverageTemperature'))

        print('Writing to Parquet ....')
        write(filename, df=df_year)
        logging.info(f'File {filename}.csv converted to Parquet.')

    elif filename == 'us-cities-demographics':

        print(f'Reading file {filename}.csv ....')

        df = read(filename, sep=';')
        df_rename = df \
            .withColumnRenamed('Median Age', 'MedianAge') \
            .withColumnRenamed('Male Population', 'MalePopulation') \
            .withColumnRenamed('Female Population', 'FemalePopulation') \
            .withColumnRenamed('Total Population', 'TotalPopulation') \
            .withColumnRenamed('Number of Veterans', 'NumberOfVeterans') \
            .withColumnRenamed('Foreign-born','ForeignBorn') \
            .withColumnRenamed('Average Household Size', 'AverageHouseholdSize') \
            .withColumnRenamed('State Code', 'StateCode') \
            
        df_rename.printSchema()

        print(f'Writing {filename} to Parquet ....')
        write(filename, df=df_rename)
        logging.info(f'File {filename}.csv converted to Parquet.')

    else:

        print(f'Reading file {filename}.csv ....')
        df = read(filename)
        print('Writing to Parquet ....')
        write(filename, df=df)
        logging.info(f'File {filename}.csv converted to Parquet.')

print('All files converted successfully.')
logging.info('All files converted successfully (logging).')

