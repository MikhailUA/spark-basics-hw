import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import concat_ws, udf, col, split, avg, round, year, month, dayofmonth
from pyspark.sql.types import StructType, StructField, LongType, StringType, FloatType

from opencage.geocoder import OpenCageGeocode
import Geohash

os.environ['GEOCODER_API_KEY'] = '5e5a289d19a54125906c378841edb0b5'


key = os.environ['GEOCODER_API_KEY']
geocoder = OpenCageGeocode(key)


@udf
def get_coordinates(query):
    try:
        results = geocoder.geocode(query, pretty=1, no_annotations=1)
        return ','.join([str(results[-1]['geometry']['lat']), str(results[-1]['geometry']['lng'])])
    except:
        return None


@udf
def get_hash(lat, lng, precision=4):
    return Geohash.encode(lat, lng, precision)

def main():
    spark = SparkSession.builder.getOrCreate()

    spark.sparkContext.setLogLevel('ERROR')

    schema = StructType([
        StructField("Id", LongType(), True),
        StructField("Name", StringType(), True),
        StructField("Country", StringType(), True),
        StructField("City", StringType(), True),
        StructField("Address", StringType(), True),
        StructField("Latitude", FloatType(), True),
        StructField("Longitude", FloatType(), True)
      ])

    df_hotels = spark.read.csv('data/hotels', header=True, schema=schema)

    df_hotels_nulls = df_hotels \
                        .filter(col('Latitude').isNull() | col('Longitude').isNull()) \
                        .withColumn('coordinates',
                            get_coordinates(
                                concat_ws(',',
                                    df_hotels.Name, df_hotels.Address, df_hotels.City, df_hotels.Country)))

    df_hotels = df_hotels\
        .filter((col('Latitude').isNotNull()) & (col('Longitude').isNotNull()))\
        .union(
            df_hotels_nulls
                .withColumn('Latitude',  split(df_hotels_nulls.coordinates, ',').getItem(0).cast('float'))
                .withColumn('Longitude', split(df_hotels_nulls.coordinates, ',').getItem(1).cast('float'))
                .drop(df_hotels_nulls.coordinates))\
        .withColumn('Geohash', get_hash(col('Latitude'), col('Longitude')))

    df_weather = spark.read\
        .option("recursiveFileLookup", "true")\
        .parquet('data/weather')

    df_weather = df_weather.withColumn('Geohash', get_hash(col('lat'), col('lng')))

    df_weather_aggr = df_weather\
                        .drop('lng','lat')\
                        .groupBy('Geohash', 'wthr_date')\
                        .agg(round(avg('avg_tmpr_f'), 1).alias('avg_tmpr_f'), round(avg('avg_tmpr_c'), 1).alias('avg_tmpr_c'))

    df_enriched_ = df_hotels.join(df_weather_aggr, 'Geohash', 'left').drop('Geohash')
    df_enriched_.show()
    df_enriched_\
        .withColumn('year', year('wthr_date'))\
        .withColumn("month", month("wthr_date"))\
        .withColumn("day", dayofmonth("wthr_date"))\
        .write.format('parquet')\
        .partitionBy('year', 'month', 'day')\
        .option('compression', 'snappy') \
        .mode('overwrite') \
        .save('data/hotels_enriched')


if __name__ == '__main__':
    main()
