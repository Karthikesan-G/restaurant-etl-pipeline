def main():
    # Databricks notebook source
    # from pyspark.sql import SparkSession
    # from pyspark.sql.functions import *
    # from pyspark.sql.types import *
    # from pyspark.sql.window import Window

    # COMMAND ----------

    spark = SparkSession.builder\
        .appName('restaurant_details_project1')\
        .getOrCreate()

    spark._jsc.hadoopConfiguration().set("fs.s3a.access.key", AWS_ACCESS_KEY)
    spark._jsc.hadoopConfiguration().set("fs.s3a.secret.key", AWS_SECRET_KEY)
    spark._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "s3.amazonaws.com")

    spark

    # COMMAND ----------

    # MAGIC %md
    # MAGIC Defining Schema

    # COMMAND ----------

    my_schema = StructType([
        StructField("_c0", StringType(), True),
        StructField("sno", IntegerType(), True),
        StructField("zomato_url", StringType(), True),
        StructField("name", StringType(), True),
        StructField("city", StringType(), True),
        StructField("area", StringType(), True),
        StructField("rating", StringType(), True),
        StructField("rating_count", IntegerType(), True),
        StructField("telephone", StringType(), True),
        StructField("cusine", StringType(), True),
        StructField("cost_for_two", StringType(), True),
        StructField("address", StringType(), True),
        StructField("coordinates", StringType(), True),
        StructField("timings", StringType(), True),
        StructField("online_order", BooleanType(), True),
        StructField("table_reservation", BooleanType(), True),
        StructField("delivery_only", BooleanType(), True),
        StructField("famous_food", StringType(), True)
    ])

    # COMMAND ----------

    # MAGIC %md
    # MAGIC Reading CSV from S3

    # COMMAND ----------

    df = spark.read.format('csv')\
        .schema(my_schema)\
        .option('header', True)\
        .option('multiLine', True)\
        .option('quote', "\"")\
        .option('escape', "\"")\
        .load('s3a://restaurant-details-dataset/rawData/india_all_restaurants_details.csv')

    df.display()

    # COMMAND ----------

    df.printSchema()

    # COMMAND ----------

    # MAGIC %md
    # MAGIC Remove Unwanted Columns

    # COMMAND ----------

    df = df.select('sno','zomato_url','name','city','area','rating','rating_count','telephone','cusine','cost_for_two','address','coordinates','timings','online_order','table_reservation','delivery_only','famous_food')

    # COMMAND ----------

    # MAGIC %md
    # MAGIC Check Nulls

    # COMMAND ----------

    for col_v in df.columns:
        print(col_v, df.filter(col(col_v).isNull()).count())

    # COMMAND ----------

    # MAGIC %md
    # MAGIC Check Dups

    # COMMAND ----------

    df.groupBy('zomato_url').agg(
        count('zomato_url').alias('count')
    ).filter(col('count') > 1).display()

    # COMMAND ----------

    # MAGIC %md
    # MAGIC Normalising rating, rating column

    # COMMAND ----------

    df = df.withColumn(
        'rating', 
        when(
            (col('rating_count') == 0 ) & 
            (col('rating').isin('Nové', 'NEW', 0, "0", "")),
            lit(None)
        ).otherwise(col('rating')))

    df.groupBy('rating').count().display()

    # COMMAND ----------

    df.groupBy('rating_count').count().display()

    # COMMAND ----------

    # MAGIC %md
    # MAGIC Spliting telephone column

    # COMMAND ----------

    df.select('telephone').count()

    # COMMAND ----------

    split_regex_from_end = r'\s((?:\d{2}\s[\d]+)|(?:\d{3}\s[\d]+)|(?:\d{4}\s[\d]+)|(?:\d{5}\s[\d]+)|(?:\d{11,})|(?:\+91\s*[\d]{10}))$'
    split_regex_from_start = r'^((?:\d{2}\s[\d]+)|(?:\d{3}\s[\d]+)|(?:\d{4}\s[\d]+)|(?:\d{5}\s[\d]+)|(?:\d{6}\s[\d]+)|(?:\d{11,})|(?:\+91\s*[\d]{10}))\s'


    df = df.withColumn(
            'telephone',
            regexp_replace(col('telephone'), split_regex_from_end, ", $1")
        )\
        .withColumn(
            'telephone',
            when(col('telephone').isin('Numer niedostępny', 'Numero Non disponibile', 'Číslo nie je dostupné', 'Not Available', 0, '0'), None)\
            .when(~col('telephone').contains(','), regexp_replace(col('telephone'), split_regex_from_start, "$1, "))\
            .otherwise(col('telephone'))
        )\
        .withColumn(
            'telephone',
            when(~col('telephone').contains(','), regexp_replace(col('telephone'), r'^((?:\d{2}\s[\d]+))', "$1, "))\
            .otherwise(col('telephone'))
        )\
        .withColumn(
            'phone', 
            when(split(col('telephone'), ', ')[0].startswith('+91'), split(col('telephone'), ', ')[0])\
            .when(split(col('telephone'), ', ')[1].startswith('+91'), split(col('telephone'), ', ')[1])\
            .otherwise(split(col('telephone'), ', ')[0])
        )\
        .withColumn(
            'phone1', 
            when(~split(col('telephone'), ', ')[1].startswith('+91'), split(col('telephone'), ', ')[1])\
            .when(~split(col('telephone'), ', ')[0].startswith('+91'), split(col('telephone'), ', ')[0])\
            .otherwise(split(col('telephone'), ', ')[1])
        )


    # COMMAND ----------

    df = df.withColumn(
            'phone',
            when(length(col('phone')) < 6, lit(col('phone1')))\
            .otherwise(lit(col('phone')))
        )\
        .withColumn(
            'phone1',
            when(col('phone') == col('phone1'), None)\
            .otherwise(lit(col('phone1')))
        )

    # COMMAND ----------

    # MAGIC %md
    # MAGIC Spliting columns and changing into array type

    # COMMAND ----------

    df = df.withColumn('cusine', array_distinct(split(col('cusine'), ', ')))\
        .withColumn('latitude', split(col('coordinates'), ',')[0].cast(FloatType()))\
        .withColumn('longitude', split(col('coordinates'), ',')[1].cast(FloatType()))\
        .withColumn('famous_food', array_distinct(split(col('famous_food'), ', ')))\
        .withColumn('address', trim(col('address')))\
        .withColumn('address', regexp_replace(col('address'), r'^\s*\,', ''))


    # COMMAND ----------

    # MAGIC %md
    # MAGIC Removing Invalid datas in lat, long

    # COMMAND ----------

    df = df.withColumn('latitude', when(col('latitude') <= 0, None).otherwise(col('latitude')))\
        .withColumn('longitude', when(col('longitude') <= 0, None).otherwise(col('longitude')))


    # COMMAND ----------

    # MAGIC %md
    # MAGIC Normalising cost column

    # COMMAND ----------

    df = df.withColumn(
            'cost_for_two',
            regexp_replace(col('cost_for_two'), r'\,', '')
        )\
        .withColumn('cost_for_two', col('cost_for_two').cast(IntegerType()))

    # COMMAND ----------

    # MAGIC %md
    # MAGIC Cleaning timings column and renameing its key names

    # COMMAND ----------

    timing_schema = MapType(StringType(), StringType())

    df = df.withColumn(
            'timings',
            from_json(
                col('timings'),
                timing_schema
            )
        )


    # COMMAND ----------

    df.createOrReplaceTempView('df_sql')

    # COMMAND ----------

    df = spark.sql('''
        select *,
        transform_keys(
            timings,
            (k, v) -> case
            WHEN k = 'Pon' THEN 'Mon'
            WHEN k = 'Wt' THEN 'Tue'
            WHEN k = 'Śr' THEN 'Wed'
            WHEN k = 'Czw' THEN 'Thu'
            WHEN k = 'Pt' THEN 'Fri'
            WHEN k = 'Sb' THEN 'Sat'
            WHEN k = 'Nd' THEN 'Sun'
            WHEN k = 'Ned' THEN 'Sun'
            WHEN k = 'Sob' THEN 'Sat'
            WHEN k = 'Mon' THEN 'Mon'
            WHEN k = 'Uto' THEN 'Tue'
            WHEN k = 'Str' THEN 'Wed'
            WHEN k = 'Štv' THEN 'Thu'
            WHEN k = 'Pia' THEN 'Fri'
            ELSE k
            end
        ) as tranfromed_timings
        from df_sql;
    ''')

    # COMMAND ----------

    # MAGIC %md
    # MAGIC Final table

    # COMMAND ----------

    df.display()

    # COMMAND ----------

    # MAGIC %md
    # MAGIC Dropping unwanted columns and rearraging them

    # COMMAND ----------

    df = df.drop('telephone', 'coordinates', 'timings')\
        .withColumnRenamed('tranfromed_timings', 'timings')\
        .select('sno','zomato_url','name','city','area','rating','rating_count','phone', 'phone1','cusine','cost_for_two','address','latitude', 'longitude', 'timings','online_order','table_reservation','delivery_only','famous_food')

    # COMMAND ----------

    df.display()

    # COMMAND ----------

    df.columns

    # COMMAND ----------

    df.groupBy("city").count().display()

    # COMMAND ----------

    # MAGIC %md
    # MAGIC Writing into S3
    # MAGIC

    # COMMAND ----------

    df.write.format('parquet')\
        .mode('overwrite')\
        .partitionBy('city')\
        .save('s3a://restaurant-details-dataset/cleanedData/')