#######################################################################################################################################
# File Name: ContextProvider.py
# Author: Ashish Tyagi
# Date created: Jan 4, 2017
# Date last modified: Feb 2, 2017
# Python Version: 2.7
# Description: Creates or reuses the spark context, spark streaming context, SQL context  
#######################################################################################################################################

from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark.sql import SQLContext
from utils import ConfigProvider

sparkContext = None
sqlContext = None
sparkStreamingContext = None

# Creates a spark context if not exists already
def getSparkInstance():
    global sparkContext
    if sparkContext == None:
        # Create Spark Context
        conf = SparkConf().setAppName(ConfigProvider.JobName)
        # for local mode use following 
        #.setMaster(ConfigProvider.ExecutionMode).set("spark.executor.instances", 3).set("spark.local.ip", "127.0.0.1")
        sparkContext = SparkContext(conf=conf)
    return sparkContext

# Checks if spark context is present if not creates it and then creates or reuses the SQL context
def getSQLContext():
    global sparkContext
    global sqlContext
    if sparkContext == None:
        getSparkInstance()
    if sqlContext == None:
        sqlContext = SQLContext(sparkContext)
    return sqlContext

# Checks if spark context is present if not creates it and then creates or reuses the spark streaming context
def getSparkStreamingContext():
    global sparkContext
    global sparkStreamingContext
    if sparkContext == None:
        getSparkInstance()
    if sparkStreamingContext == None:
        sparkStreamingContext = StreamingContext(sparkContext , int(ConfigProvider.StreamingInterval))
    return sparkStreamingContext