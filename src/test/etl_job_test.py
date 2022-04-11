import unittest
from src.main.python.etl_job import init_spark, transform_hotels_data, transform_weather_data
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, LongType, StringType, FloatType


class TestETLMethods(unittest.TestCase):

    def test_init_spark(self):
        self.assertIsInstance(init_spark(), SparkSession)

    def test_transform_hotels_data(self):
        spark = init_spark()
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

        df = spark.read.csv('src/test/data/hotels', header=True, schema=schema)
        self.assertIsInstance(df, DataFrame)

        df_t = transform_hotels_data(df)
        self.assertLessEqual(df_t.count(), df.count())
        self.assertEqual(df_t.count(), df_t.dropna().count())
        self.assertTrue('Geohash' in df_t.columns)

    def test_transform_weather_data(self):
        spark = init_spark()
        spark.sparkContext.setLogLevel('ERROR')

        df = spark.read.option("recursiveFileLookup", "true").parquet('src/test/data/weather')
        self.assertIsInstance(df, DataFrame)

        df_t = transform_weather_data(df)
        self.assertLessEqual(df_t.count(), df.count())
        self.assertEqual(df_t.count(), df_t.dropna().count())
        self.assertTrue('Geohash' in df_t.columns)


if __name__ == '__main__':
    unittest.main()
