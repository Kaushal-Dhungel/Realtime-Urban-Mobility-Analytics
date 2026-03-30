from pyspark import pipelines as dp
from pyspark.sql.functions import *
from pyspark.sql.types import *


# Event Hubs configuration
EH_NAMESPACE                    = "dataEngEvents"
EH_NAME                         = "engevent"


EH_CONN_STR                     = spark.conf.get("conn_string")
# Kafka Consumer configuration

KAFKA_OPTIONS = {
  "kafka.bootstrap.servers"  : f"{EH_NAMESPACE}.servicebus.windows.net:9093",
  "subscribe"                : EH_NAME,
  "kafka.sasl.mechanism"     : "PLAIN",
  "kafka.security.protocol"  : "SASL_SSL",
  "kafka.sasl.jaas.config"   : f"kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username=\"$ConnectionString\" password=\"{EH_CONN_STR}\";",
  "kafka.request.timeout.ms" : 10000,
  "kafka.session.timeout.ms" : 10000,
  "maxOffsetsPerTrigger"     : 10000,
  "failOnDataLoss"           : 'true',
  "startingOffsets"          : 'earliest'
}

# this creates a "Table" in the "bronze" layer
@dp.table
def rides_raw():
    df = spark.readStream.format("kafka")\
            .options(**KAFKA_OPTIONS)\
            .load()
            
    # convert encoded values to string and write to "rides" column
    df = df.withColumn("rides", col("value").cast("string"))
    return df

