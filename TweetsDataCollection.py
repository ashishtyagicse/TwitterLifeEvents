#######################################################################################################################################
# File Name: TweetsDataCollection.py
# Author: Ashish Tyagi
# Date created: Jan 18, 2017
# Date last modified: Feb 2, 2017
# Python Version: 2.7
# Description: Collects tweets from twitter using twitter search API.
#              collects 10 tweets per life event defined in LifeEventsList file and 5 tweets for related stem words
#              also collects user information from these tweets and adds them to TwitterUsers.json 
#              Finally adds 5 tweet per user creating a nice pool of mixed tweets. 
#######################################################################################################################################

import json
from utils import ConfigProvider, FileContentLoader
from twitter import OAuth, Twitter, TwitterStream

if __name__ == '__main__':
    
    AllTweets = None
    AllUsers = {}
    AllNonEventTweets = None
    
    # Initiate the connection to Twitter Streaming / search API
    oauth = OAuth(ConfigProvider.AccessToken, ConfigProvider.AccessSecret, ConfigProvider.ConsumerKey, ConfigProvider.ConsumberSecret)
    twitter_stream = TwitterStream(auth=oauth)
    twitter=Twitter(auth=oauth)
    
    # Load life events detail and collect tweets for each life event
    LifeEventsList = FileContentLoader.LifeEventsList()
    for LifeEvent in  LifeEventsList["LifeEventList"]:
        # Collect 10 tweets for current life event
        # print "Processing tweets for life event:" , LifeEvent["Topic"]
        EventTweets = twitter.search.tweets(q=LifeEvent["Event"], lang='en', count=10)
        # Collect 5 tweets per stem word for this life event category
        for StemWord in LifeEvent["StemWords"]:
            EventStemWordTweets = twitter.search.tweets(q=StemWord, lang='en', count=5)
        EventTweets["statuses"] += EventStemWordTweets["statuses"]
        if AllTweets is None:
            AllTweets = EventTweets
        else:
            AllTweets["statuses"] += EventTweets["statuses"]
    
     
    # Collect user details from tweets so far
    for Status in AllTweets["statuses"]:
        if Status["user"]["id"] not in AllUsers:
            AllUsers[Status["user"]["id"]] = { "screen_name" : Status["user"]["screen_name"] , "name" : Status["user"]["name"] }
            # Collect 5 more random tweets for current user 
            UserTweets = twitter.statuses.user_timeline(user_id = Status["user"]["id"], count = 5 )
            if AllNonEventTweets is None:
                AllNonEventTweets = UserTweets
            else:
                AllNonEventTweets += UserTweets
    
    AllTweets["statuses"] += AllNonEventTweets
    # Save user details and tweets to disk
    # print "Writing tweets to disks"
    UserFile = open(ConfigProvider.UserFilePath,'w') 
    UserFile.write(json.dumps(AllUsers))
    UserFile.close()
    TweetsFile = open(ConfigProvider.TweetsFilePath,'w')
    TweetsFile.write(json.dumps(AllTweets["statuses"]))
    TweetsFile.close()