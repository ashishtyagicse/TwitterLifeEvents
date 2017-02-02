import json
import sys
from utils import ConfigProvider, Cleaner, ContextProvider

# def LoadTweetsFromFile():
#     #try:
#         SparkContext = ContextProvider.getSparkInstance()
#         SqlContext = ContextProvider.getSQLContext()
#         SparkStreamingContext = ContextProvider.getSparkStreamingContext()
#         #SparkContext.setLogLevel("TRACE")
#         TweetsFileRdd = SparkContext.wholeTextFiles(ConfigProvider.TweetsFilePath).values()
#         TweetsDataFrame = SqlContext.read.json(TweetsFileRdd)
#         TweetsStreamedRdd = SparkStreamingContext.queueStream([TweetsDataFrame.rdd.collect()], True)
#         #ProcessedTweets = TweetsStreamedRdd.flatMap(lambda TwitterFeed : TwitterFeed.statuses).map(lambda status: processTweets(status))
#         ProcessedTweets = TweetsStreamedRdd.map(lambda status: processTweets(status))
#         return ProcessedTweets
#     #except:
#         print "Error loading tweets from json file\n " , sys.exc_info()[0]

def LoadTweetsFromFile():
    try:
        SparkContext = ContextProvider.getSparkInstance()
        SparkStreamingContext = ContextProvider.getSparkStreamingContext()
        #SparkContext.setLogLevel("TRACE")
        Tweets = json.load(open(ConfigProvider.TweetsFilePath))
        TweetRdds = []
        for Tweet in Tweets:
            TweetRdds += [SparkContext.parallelize([Tweet])]
        TweetsStreamedRdd = SparkStreamingContext.queueStream(TweetRdds, True)
        ProcessedTweets = TweetsStreamedRdd.map(lambda status: processTweets(status))
        return ProcessedTweets
    except:
        print "Error loading tweets from json file\n " , sys.exc_info()[0]

        
def processTweets(RawTweet):
    try:  
        RawTweet["text"] = RawTweet["text"].encode("ascii","ignore")
        for symbol in RawTweet["entities"]["symbols"]:
            StartIndex = int(symbol["indices"][0])
            EndIndex = int(symbol["indices"][1])
            ReplaceLen = EndIndex - StartIndex
            RawTweet["text"] = RawTweet["text"][ : StartIndex ] + ReplaceLen * " " +  RawTweet["text"][EndIndex : ]
        for hashtag in RawTweet["entities"]["hashtags"]:
            StartIndex = int(hashtag["indices"][0])
            EndIndex = int(hashtag["indices"][1])
            ReplaceLen = EndIndex - StartIndex
            RawTweet["text"] = RawTweet["text"][ : StartIndex ] + ReplaceLen * " " + RawTweet["text"][EndIndex : ]
        for url in RawTweet["entities"]["urls"]:
            StartIndex = int(url["indices"][0])
            EndIndex = int(url["indices"][1])
            ReplaceLen = EndIndex - StartIndex
            RawTweet["text"] = RawTweet["text"][ : StartIndex ] + ReplaceLen * " " + RawTweet["text"][EndIndex : ]
        if "media" in RawTweet["entities"] and RawTweet["entities"]["media"] is not None:
            for MediaUrl in RawTweet["entities"]["media"]:
                RawTweet["text"] = RawTweet["text"].replace(MediaUrl["url"],"")
        for user in RawTweet["entities"]["user_mentions"]:
            RawTweet["text"] = RawTweet["text"].replace("@"+user["screen_name"] + ":" , " " + user["name"].encode("ascii","ignore") + " ")
        RawTweet["text"] = Cleaner.TextCleaner(RawTweet["text"])
        return RawTweet
    except:
        print "Error cleaning tweets from json file\n " , sys.exc_info()[0]