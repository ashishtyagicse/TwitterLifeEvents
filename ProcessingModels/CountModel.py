from utils import ContextProvider, FileContentLoader, ConfigProvider, Cleaner, HbaseSave
from datetime import datetime


def CountModel(StatusDataRdd):
	
	SparkContext = ContextProvider.getSparkInstance()
	SqlContext = ContextProvider.getSQLContext()
	StopWordsList = SparkContext.broadcast(FileContentLoader.LoadStopWords())
	ApostropesReplaceList = SparkContext.broadcast(FileContentLoader.LoadApostropesReplaceWords())
	LifeEventsList = SparkContext.broadcast(FileContentLoader.LifeEventsList())
	
	StatusData = StatusDataRdd.collect()[0]
	ClanedTweetText = Cleaner.ReplaceApostropes(ApostropesReplaceList.value , Cleaner.RemovePunctuations(StatusData["text"]))
	BareTweetWords = Cleaner.RemoveStopWords(StopWordsList.value, ClanedTweetText).split()
	DetectedTopic = []
	TweetDate = datetime.strptime(StatusData["created_at"][:19] + StatusData["created_at"][25:], '%a %b %d %X %Y')
	
	for Topic in LifeEventsList.value["topic_list"]:
		if Topic["topic"] in BareTweetWords or Topic["stem_words"] in BareTweetWords:		
			DetectedTopic.append( {"topic" : Topic["topic"], "date" : TweetDate.strftime('%m/%d/%Y') , "Id": StatusData["id"]} )
		
	
	if DetectedTopic == []:
		DetectedTopic.append( {"topic" : "None", "date" : TweetDate.strftime('%m/%d/%Y') , "Id": StatusData["id"]} )
	print DetectedTopic
	TweetDataFrame = SqlContext.createDataFrame(SparkContext.parallelize(DetectedTopic))
	TweetDataFrame.write.mode('append').parquet(ConfigProvider.OutputParquetFilePath)
# for saving to HBase	
# 	HBaseInsert = []
# 	for Topic in DetectedTopic:
# 		HBaseInsert += [(Topic["Id"] ,[Topic["Id"], "cf", "Topic", Topic["topic"]])]
# 		HBaseInsert += [(Topic["Id"] ,[Topic["Id"], "cf", "Date", Topic["date"]])]
# 	print HBaseInsert
# 	HbaseSave.SaveRecord(SparkContext.parallelize(HBaseInsert), "LifeEventCount")