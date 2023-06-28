from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql import functions as f
from pyspark.sql import DataFrameWriter

jdbc_config = {
    'url_stage': 'jdbc:postgresql://postgres_database:5433/stage',
    'url_warehouse': 'jdbc:postgresql://postgres_database:5433/warehouse',
    'user': 'bmk',
    'password': 'bmk',
    'driver': 'org.postgresql.Driver'
}
spark = SparkSession.builder.getOrCreate()

#airflow_jinja_ds = spark.conf.get('spark.driver.airflow_jinja_ds')

df = spark.read \
    .format("jdbc") \
    .option('user', jdbc_config['user']) \
    .option('password', jdbc_config['password']) \
    .option("driver", jdbc_config['driver']) \
    .option("url", jdbc_config['url_stage']) \
    .option("query",
            f"SELECT json_data -> 'data' AS json FROM covid19_stage WHERE date_of_data = '2023-03-02'") \
    .load()

schema_array = ArrayType(StructType([
    StructField("date", DateType(), True),
    StructField('active', LongType(), True),
    StructField('deaths', LongType(), True),
    StructField('region', StructType([
        StructField('iso', StringType(), True),
        StructField('lat', DecimalType(), True),
        StructField('long', DecimalType(), True),
        StructField('name', StringType(), True),
        StructField('cities', ArrayType(StringType()), True),
        StructField('province', StringType(), True)
    ]), True),
    StructField('confirmed', LongType(), True),
    StructField('recovered', LongType(), True),
    StructField('active_diff', LongType(), True),
    StructField('deaths_diff', LongType(), True),
    StructField('last_update', StringType(), True),
    StructField('fatality_rate', DoubleType(), True),
    StructField('confirmed_diff', LongType(), True),
    StructField('recovered_diff', LongType(), True)
]))

df = df.select(f.from_json(f.col('json'), schema=schema_array, options={'multiLine': 'true'}).alias('json'))

df = df.select(f.explode('json')).repartition(2)

list_of_columns = ['col.date', 'col.confirmed', 'col.confirmed_diff', 'col.deaths', 'col.deaths_diff', 'col.recovered',
                   'col.recovered_diff', 'col.active', 'col.active_diff', 'col.fatality_rate', 'col.region.name',
                   'col.region.province', 'col.region.lat', 'col.region.long']

df = df.select(list_of_columns).filter((f.col('province') != 'Unknown') & (f.col('province') != 'Recovered')). \
    withColumnRenamed('date', 'date_day').withColumnRenamed('name', 'country')

writer = DataFrameWriter(df)
writer.jdbc(url=jdbc_config['url_warehouse'],
            table='covid19_warehouse',
            mode='append',
            properties={'user': jdbc_config['user'],
                        'password': jdbc_config['password'],
                        'driver': jdbc_config['driver']})

spark.sparkContext.stop()
