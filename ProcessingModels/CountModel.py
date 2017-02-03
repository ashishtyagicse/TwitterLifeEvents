#######################################################################################################################################
# File Name: CountModel.py
# Author: Ashish Tyagi
# Date created: Jan 4, 2017
# Date last modified: Feb 2, 2017
# Python Version: 2.7
# Description: Identify if a tweet relates to a particular life event and outputs its id and tweet date to parquet file which 
#              can then be used to create a count per life event per date chart.  
#######################################################################################################################################

from utils import ContextProvider, FileContentLoader, ConfigProvider, Cleaner, HbaseSave
from datetime import datetime
from pyspark.sql.functions import coalesce

def CountModel(StatusDataRdd):
	# Create or get spark and SQL Context and broadcast stop words, Apostrophes word and life events list.
	SparkContext = ContextProvider.getSparkInstance()
	SqlContext = ContextProvider.getSQLContext()
	StopWordsList = SparkContext.broadcast(FileContentLoader.LoadStopWords())
	ApostrophesReplaceList = SparkContext.broadcast(FileContentLoader.LoadApostrophesReplaceWords())
	LifeEventsList = SparkContext.broadcast(FileContentLoader.LifeEventsList())
	
	# Clean the tweet text and get a barebone words list with only very important words
	StatusData = StatusDataRdd.collect()[0]
	ClanedTweetText = Cleaner.ReplaceApostrophes(ApostrophesReplaceList.value , Cleaner.RemovePunctuations(StatusData["text"]))
	BareTweetWords = Cleaner.RemoveStopWords(StopWordsList.value, ClanedTweetText).split()
	DetectedTopic = []
	# Get the tweets date
	TweetDate = datetime.strptime(StatusData["created_at"][:19] + StatusData["created_at"][25:], '%a %b %d %X %Y')
	
	# Identify if a tweet belongs to a particular life event 
	for Event in LifeEventsList.value["LifeEventList"]:
		if Event["Event"] in BareTweetWords or Event["StemWords"] in BareTweetWords:		
			DetectedTopic.append( {"Event" : Event["Event"], "date" : TweetDate.strftime('%m/%d/%Y') , "Id": StatusData["id"]} )
		
	# saves none if a tweet does not belongs to any life event
	if DetectedTopic == []:
		DetectedTopic.append( {"Event" : "None", "date" : TweetDate.strftime('%m/%d/%Y') , "Id": StatusData["id"]} )
	
	# Saves output in a parquet file
	TweetDataFrame = SqlContext.createDataFrame(SparkContext.parallelize(DetectedTopic))
	TweetDataFrame.coalesce(ConfigProvider.MaxPartFiles).write.mode('append').parquet(ConfigProvider.OutputParquetFilePath)

# for saving to HBase	
# 	HBaseInsert = []
# 	for Topic in DetectedTopic:
# 		HBaseInsert += [(Topic["Id"] ,[Topic["Id"], "cf", "Topic", Topic["topic"]])]
# 		HBaseInsert += [(Topic["Id"] ,[Topic["Id"], "cf", "Date", Topic["date"]])]
# 	print HBaseInsert
# 	HbaseSave.SaveRecord(SparkContext.parallelize(HBaseInsert), "LifeEventCount")