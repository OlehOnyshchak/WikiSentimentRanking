from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql.functions import udf
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructField, StructType, ArrayType, LongType, DoubleType

class SparkSentimentStreamer(object):

    def __init__(self, sc, ssc, spark, process_func, input_dir, out_dir):
        self.sc = sc
        self.ssc = ssc
        self.spark = spark
        self.process_func = process_func
        self.input_dir = input_dir
        self.out_dir = out_dir

    def process_rdd(self, rdd, process_func, out_dir):
        if not rdd.isEmpty():
            df = self.spark.read.json(rdd, multiLine=True)
            df.show(5)
            func_udf = udf(process_func, DoubleType())
            df_processed = df.select("title", "url", func_udf(df.text).alias("sentiment"))
            df_processed.show(5)
            rows = df_processed.count()
            df_processed.repartition(rows).write.json(out_dir)

    def run(self):
        fileStream = self.ssc.textFileStream(self.input_dir)
        fileStream.foreachRDD(lambda record: self.process_rdd(record, self.process_func, self.out_dir))
        self.ssc.start()             
        self.ssc.awaitTermination()
    
    def stop(self):
        self.ssc.stop()