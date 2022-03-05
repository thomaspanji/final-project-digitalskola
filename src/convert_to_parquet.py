from pyspark.sql import SparkSession
from pyspark.sql.functions import year
from pyspark.sql.functions import avg
from google.cloud import storage


# Create function for reading csv files
def read(filename,sep=None):
    """ Function to read CSV file.
    
    CSV file located in Google Cloud Storage with prefix 'input/' acted as 
    raw file. The output is a Spark dataframe.
    """

    print(f'Reading file {filename}.csv ....')

    if sep is None:
        df = spark.read \
            .option('header', 'true') \
            .option('inferSchema', 'true') \
            .csv(f'gs://final_project_de/input/{filename}.csv')

    else:
        df = spark.read \
            .option('sep', sep) \
            .option('header', 'true') \
            .option('inferSchema', 'true') \
            .csv(f'gs://final_project_de/input/{filename}.csv')

    # Printing Spark DataFrame inferred schema
    df.printSchema()
    return df


# Create function to write file into parquet format
def write(filename, df=None, partitionBy=None):
    """ Function to write dataframe into Parquet file.
    
    The Parquet file is saved into Google Cloud Storage as staging phase with 
    added prefix 'staging/'.
    """
    
    # Create filename for written to Parquet
    filename = filename.replace('-', '_').lower()
    print(f'Writing to Parquet with name {filename}.parquet ....')
    
    if df is not None:
        df_to_parquet = df
    else:
        df_to_parquet = read(filename)

    if partitionBy is None:
        df_to_parquet.write \
            .mode('overwrite') \
            .parquet(f'gs://final_project_de/staging/{filename}.parquet')
    else:
        df_to_parquet.write \
            .partitionBy(partitionBy) \
            .mode('overwrite') \
            .parquet(f'gs://final_project_de/staging/{filename}.parquet')

    print(f'Converted into {filename}.parquet')


# Create Spark Session
spark = SparkSession.builder \
    .appName("Convert to Parquet") \
    .getOrCreate()

# Create client to access project's bucket in Google Cloud Storage 
client = storage.Client(project='final-project-de')
blobs = client.get_bucket('final_project_de').list_blobs(prefix='input/')
files = [file.name for file in list(blobs)]

# Process each file in the bucket
for file in files:

    filename = file.replace('input/', '').replace('.csv','')

    if filename == 'GlobalLandTemperaturesByCity':
        df = read(filename)
        df_year = df.withColumn('year', year('dt')) \
            .select('year','AverageTemperature','City','Country','Latitude','Longitude') \
            .groupBy('year','City','Country','Latitude','Longitude') \
            .agg(avg('AverageTemperature').alias('AverageTemperature'))

        write(filename, df=df_year)
        
    elif filename == 'us-cities-demographics':
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
        write(filename, df=df_rename)

    else:
        df = read(filename)
        write(filename, df=df)
        
print('All files converted successfully.')


