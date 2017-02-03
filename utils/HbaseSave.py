#######################################################################################################################################
# File Name: HbaseSave.py
# Author: Ashish Tyagi
# Date created: Jan 18, 2017
# Date last modified: Feb 2, 2017
# Python Version: 2.7
# Description: saves the processed output from a model in HBase   
#######################################################################################################################################

from utils import ConfigProvider, ContextProvider

# Function saveRecord converts each Rdd in form of a put record (id, [id, Column Family, Column, Value] )
# Then inserts it in the hbase table as specified in the configuration structure 
def SaveRecord(rdd, HbaseTable):
    # Hbase Host
    host = ConfigProvider.HbaseHost
    
    keyConv = "org.apache.spark.examples.pythonconverters.StringToImmutableBytesWritableConverter"
    valueConv = "org.apache.spark.examples.pythonconverters.StringListToPutConverter"

    # Configuration for saveAsNewAPIHadoopDataset
    conf = { "hbase.zookeeper.quorum": host,
             "hbase.mapred.outputtable": HbaseTable,
             "mapreduce.outputformat.class": "org.apache.hadoop.hbase.mapreduce.TableOutputFormat",
             "mapreduce.job.output.key.class": "org.apache.hadoop.hbase.io.ImmutableBytesWritable",
             "mapreduce.job.output.value.class": "org.apache.hadoop.io.Writable" }
    
    # Table specific formating of input data
    datamap = rdd
    if HbaseTable == "TweetsLifeEvent":
        datamap = rdd.map(lambda y: (y.split(',')[0], [y.split(',')[0], "measurement", "charges", y]))    
    datamap.saveAsNewAPIHadoopDataset(conf = conf, keyConverter = keyConv, valueConverter = valueConv)

