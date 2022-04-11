import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import concat_ws, udf, col, split, avg, round, year, month, dayofmonth
from pyspark.sql.types import StructType, StructField, LongType, StringType, FloatType

from opencage.geocoder import OpenCageGeocode
import geohash


def main():
    """Main ETL script

    :return: None
    """

    # get spark session
    spark = init_spark()
    set_storage_config(spark)

    # extract and transform hotels data
    df_hotels = extract_hotels_data(spark)
    df_hotels = transform_hotels_data(df_hotels)

    # extract and transform weather data
    df_weather = extract_weather_data(spark)
    df_weather = transform_weather_data(df_weather)

    df_enriched = df_hotels.join(df_weather, 'Geohash', 'left').drop('Geohash').dropna()

    # load
    load_data(df_enriched)

    spark.stop()

    return None


def init_spark():
    """Init spark session

    :return: Spark Session Object
    """
    spark = SparkSession.builder.getOrCreate()

    return spark


def set_storage_config(spark):
    """Set data source and data warehouse access config

    :return: NoneType
    """

    # data source
    spark.conf.set("fs.azure.account.auth.type.bd201stacc.dfs.core.windows.net", "OAuth")
    spark.conf.set("fs.azure.account.oauth.provider.type.bd201stacc.dfs.core.windows.net",
                   "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
    spark.conf.set("fs.azure.account.oauth2.client.id.bd201stacc.dfs.core.windows.net",
                   os.environ['DATA_SOURCE_CLIENTID'])
    spark.conf.set("fs.azure.account.oauth2.client.secret.bd201stacc.dfs.core.windows.net",
                   os.environ['DATA_SOURCE_SECRET'])
    spark.conf.set("fs.azure.account.oauth2.client.endpoint.bd201stacc.dfs.core.windows.net",
                   "https://login.microsoftonline.com/b41b72d0-4e9f-4c26-8a69-f949f367c91d/oauth2/token")

    # data warehouse
    spark.conf.set(f"fs.azure.account.key.{os.environ['DATA_WAREHOUSE_ACCOUNT_NAME']}.blob.core.windows.net",
                   os.environ['DATA_WAREHOUSE_ACCOUNT_KEY'])


def extract_hotels_data(spark):
    """Extract hotels data from the source

    :param: Spark Session Object
    :return: Spark DataFrame
    """
    schema = StructType([
        StructField("Id", LongType(), True),
        StructField("Name", StringType(), True),
        StructField("Country", StringType(), True),
        StructField("City", StringType(), True),
        StructField("Address", StringType(), True),
        StructField("Latitude", FloatType(), True),
        StructField("Longitude", FloatType(), True)
      ])

    df_hotels = spark.read.csv('abfss://m06sparkbasics@bd201stacc.dfs.core.windows.net/hotels', header=True, schema=schema)

    return df_hotels


def transform_hotels_data(df):
    """Transform hotels data, add geohash

    :param: Spark DataFrame
    :return: Spark DataFrame
    """
    df_hotels_nulls = df \
        .filter(col('Latitude').isNull() | col('Longitude').isNull()) \
        .withColumn('coordinates',
            get_coordinates(
                concat_ws(',',
                    df.Name, df.Address, df.City, df.Country)))

    df_hotels = df\
        .filter((col('Latitude').isNotNull()) & (col('Longitude').isNotNull()))\
        .union(
            df_hotels_nulls
                .withColumn('Latitude',  split(df_hotels_nulls.coordinates, ',').getItem(0).cast('float'))
                .withColumn('Longitude', split(df_hotels_nulls.coordinates, ',').getItem(1).cast('float'))
                .drop(df_hotels_nulls.coordinates))\
        .withColumn('Geohash', get_hash(col('Latitude'), col('Longitude')))

    return df_hotels


def extract_weather_data(spark):
    """Extract weather data from the source

    :param: Spark Session Object
    :return: Spark DataFrame
    """
    df_weather = spark.read\
        .option("recursiveFileLookup", "true")\
        .parquet('abfss://m06sparkbasics@bd201stacc.dfs.core.windows.net/weather')
    return df_weather


def transform_weather_data(df):
    """Transform weather data, add geohash

    :param: Spark DataFrame
    :return: Spark DataFrame
    """
    df_weather_aggr = df.withColumn('Geohash', get_hash(col('lat'), col('lng')))\
                        .drop('lng', 'lat')\
                        .groupBy('Geohash', 'wthr_date')\
                        .agg(round(avg('avg_tmpr_f'), 1).alias('avg_tmpr_f'), round(avg('avg_tmpr_c'), 1).alias('avg_tmpr_c'))
    return df_weather_aggr


def load_data(df):
    """Load data to storage

    :param: Spark DataFrame
    :return: None
    """
    df \
        .withColumn('year', year('wthr_date'))\
        .withColumn("month", month("wthr_date"))\
        .withColumn("day", dayofmonth("wthr_date"))\
        .write.format('parquet')\
        .partitionBy('year', 'month', 'day')\
        .option('compression', 'snappy') \
        .mode('overwrite') \
        .save('wasbs://m06sparkbasics@sparkbasicsstorage1.blob.core.windows.net/hotels_enriched')


@udf
def get_coordinates(query):
    """Get geo coordinates from hotel address

    :param: string
    :return: string/None
    """
    try:
        geocoder = OpenCageGeocode(os.environ['GEOCODER_API_KEY'])
        results = geocoder.geocode(query, pretty=1, no_annotations=1)
        return ','.join([str(results[-1]['geometry']['lat']), str(results[-1]['geometry']['lng'])])
    except:
        return None


@udf
def get_hash(lat, lng, precision=4):
    """Transform latitude and longitude into 4 characters hash

    :param: float
    :param: float
    :return: string
    """
    return geohash.encode(lat, lng, precision)


if __name__ == '__main__':
    try:
        main()
        sys.exit(0)
    except Exception as e:
        print(str(e))
        sys.exit(1)
