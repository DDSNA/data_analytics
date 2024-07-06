# from kafka import KafkaConsumer, KafkaProducer
# import logging
#
# logger = logging.getLogger(__name__)
# logging.basicConfig(level=logging.DEBUG, filename='app_kfk.log', filemode='w',
#                     format='%(name)s - %(levelname)s - %(message)s')
# producer = KafkaProducer(bootstrap_servers='172.25.98.94:29092')
# consumer = KafkaConsumer('test', bootstrap_servers='172.25.98.94:49092')
#
# producer.send('Jim_Topic', b'Message from PyCharm')
# producer.send('Jim_Topic', key=b'message-two', value=b'This is Kafka-Python')
# producer.flush()
# producer.close()
# for message in consumer:
#     print(message.value)
#     logging.warning(f"Received message: {message.value}")
#     consumer.close()
#
from datetime import datetime, date

import pyspark
from pyspark.sql import Row, SparkSession
spark :SparkSession = (SparkSession.builder.appName("TestApp2")
                    .master("spark://192.168.0.38:7077")
                    .enableHiveSupport()
                    .remote("sc://192.168.0.38:15002")
                    .getOrCreate())

import numpy as np
from numpy.random import default_rng
import pandas as pd
list = range(20000)
matrix = np.vander(list, 7)
df = pd.DataFrame(matrix[:, 1:], columns=["P5", "P4", "P3", "P2", "P1", "P0"])
df.index = matrix[:, 0]  # Set the index to the "Identifier" column
df['Index'] = range(len(df))
print(df)

spark_df = spark.createDataFrame(data=df, schema=df.columns.tolist())
example_df = spark_df.rdd
print("Spark DataFrame")
print(spark_df.show())

spark_df_count = spark_df.count()
print(spark_df_count)

spark_df.cache()

try:
    from pyspark.sql.types import StructType, StructField, LongType

    # Define your schema
    schema = StructType([
        StructField("P5", LongType()),
        StructField("P4", LongType()),
        StructField("P3", LongType()),
        StructField("P2", LongType()),
        StructField("P1", LongType()),
        StructField("P0", LongType()),
        StructField("Index", LongType())
    ])

    # Create the RDD
    doubled_rdd: pyspark.RDD= spark_df.parallelize(df)
    doubled_rdd.cache()
    doubled_rdd.collect()

    # Collect and print the results
    doubled_rdd.write.saveAsTable("Table_rdd")
    doubled_rdd.saveAsHadoopFile(path="../data/orc"
                                 , outputFormatClass="org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat")
    doubled_rdd.saveAsTextFile(path="../data/text")
    print(doubled_rdd.collect())
except Exception as e:
    print("Met exception: ", e)
    spark.stop()
spark.stop()
