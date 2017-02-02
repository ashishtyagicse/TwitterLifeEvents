from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark.sql import SQLContext
from utils import ConfigProvider

sparkContext = None
sqlContext = None
sparkStreamingContext = None

def getSparkInstance():
    global sparkContext
    if sparkContext == None:
        # Create Spark Context
        conf = SparkConf().setAppName(ConfigProvider.JobName)
        #.setMaster(ConfigProvider.ExecutionMode).set("spark.executor.instances", 3).set("spark.local.ip", "127.0.0.1")
        sparkContext = SparkContext(conf=conf)
    return sparkContext

def getSQLContext():
    global sparkContext
    global sqlContext
    if sparkContext == None:
        getSparkInstance()
    if sqlContext == None:
        sqlContext = SQLContext(sparkContext)
    return sqlContext

def getSparkStreamingContext():
    global sparkContext
    global sparkStreamingContext
    if sparkContext == None:
        getSparkInstance()
    if sparkStreamingContext == None:
        sparkStreamingContext = StreamingContext(sparkContext , int(ConfigProvider.StreamingInterval))
    return sparkStreamingContext