from utils import ConfigProvider, ContextProvider

### Function saveRecord converts each Rdd in for of a put record (id, [id, Column Family, Column, Value] )
### Then inserts it in the hbase table as specified in the configuration struecture
### 
def SaveRecord(rdd, HbaseTable):
    ## Hbase Host
    host = ConfigProvider.HbaseHost

    keyConv = "org.apache.spark.examples.pythonconverters.StringToImmutableBytesWritableConverter"
    valueConv = "org.apache.spark.examples.pythonconverters.StringListToPutConverter"

    ## Configuration for saveAsNewAPIHadoopDataset
    conf = { "hbase.zookeeper.quorum": host,
             "hbase.mapred.outputtable": HbaseTable,
             "mapreduce.outputformat.class": "org.apache.hadoop.hbase.mapreduce.TableOutputFormat",
             "mapreduce.job.output.key.class": "org.apache.hadoop.hbase.io.ImmutableBytesWritable",
             "mapreduce.job.output.value.class": "org.apache.hadoop.io.Writable" }
    
    datamap = rdd
    if HbaseTable == "TweetsLifeEvent":
        datamap = rdd.map(lambda y: (y.split(',')[0], [y.split(',')[0], "measurement", "charges", y]))    
    datamap.saveAsNewAPIHadoopDataset(conf = conf, keyConverter = keyConv, valueConverter = valueConv)

