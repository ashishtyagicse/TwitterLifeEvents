##########################################################################################################################################
# File Name: TwitterDataProvider.py
# Author: Ashish Tyagi
# Date created: Jan 4, 2017
# Date last modified: Feb 2, 2017
# Python Version: 2.7
# Description: Reads a JSON file having all the tweets and creates a queuestream out of it to be processed by a spark streaming
#              applications. Also cleans the tweet text a bit to remove unwanted symbols hashtags and replaces user tags with their names  
##########################################################################################################################################

import json
import sys
from utils import ConfigProvider, Cleaner, ContextProvider

# Use this function if running spark application instead of spark streaming application 
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


# Loads tweets from file and creates RDD's from them
def LoadTweetsFromFile():
    try:
        # Get spark and spark streaming contexts 
        SparkContext = ContextProvider.getSparkInstance()
        SparkStreamingContext = ContextProvider.getSparkStreamingContext()
        # For debugging enable following  
        # SparkContext.setLogLevel("TRACE")
        
        # Loads Json having all the tweets that needs processing 
        Tweets = json.load(open(ConfigProvider.TweetsFilePath))
        TweetRdds = []
        for Tweet in Tweets:
            TweetRdds += [SparkContext.parallelize([Tweet])]
        
        TweetsStreamedRdd = SparkStreamingContext.queueStream(TweetRdds, True)
        # Cleans up the tweet text
        ProcessedTweets = TweetsStreamedRdd.map(lambda status: processTweets(status))
        return ProcessedTweets
    except:
        print "Error loading tweets from json file\n " , sys.exc_info()[0]

# Cleans tweet text, remove unwanted symbols like hashtags and replaces user tags with their names        
def processTweets(RawTweet):
    try:  
        RawTweet["text"] = RawTweet["text"].encode("ascii","ignore")
        # Remove Symbols from tweet text 
        for symbol in RawTweet["entities"]["symbols"]:
            StartIndex = int(symbol["indices"][0])
            EndIndex = int(symbol["indices"][1])
            ReplaceLen = EndIndex - StartIndex
            RawTweet["text"] = RawTweet["text"][ : StartIndex ] + ReplaceLen * " " +  RawTweet["text"][EndIndex : ]
        # Remove Hashtags from tweet text 
        for hashtag in RawTweet["entities"]["hashtags"]:
            StartIndex = int(hashtag["indices"][0])
            EndIndex = int(hashtag["indices"][1])
            ReplaceLen = EndIndex - StartIndex
            RawTweet["text"] = RawTweet["text"][ : StartIndex ] + ReplaceLen * " " + RawTweet["text"][EndIndex : ]
        # Remove Url's from tweet text
        for url in RawTweet["entities"]["urls"]:
            StartIndex = int(url["indices"][0])
            EndIndex = int(url["indices"][1])
            ReplaceLen = EndIndex - StartIndex
            RawTweet["text"] = RawTweet["text"][ : StartIndex ] + ReplaceLen * " " + RawTweet["text"][EndIndex : ]
        # Remove media content like image links from tweet text
        if "media" in RawTweet["entities"] and RawTweet["entities"]["media"] is not None:
            for MediaUrl in RawTweet["entities"]["media"]:
                RawTweet["text"] = RawTweet["text"].replace(MediaUrl["url"],"")
        # Replaces user mentions with their screen names 
        for user in RawTweet["entities"]["user_mentions"]:
            RawTweet["text"] = RawTweet["text"].replace("@"+user["screen_name"] + ":" , " " + user["name"].encode("ascii","ignore") + " ")
        # Performs a general text cleaning removing unwanted characters 
        RawTweet["text"] = Cleaner.TextCleaner(RawTweet["text"])
        return RawTweet
    except:
        print "Error cleaning tweets from json file\n " , sys.exc_info()[0]