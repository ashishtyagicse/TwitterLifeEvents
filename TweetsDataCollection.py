import json
from utils import ConfigProvider, FileContentLoader
from twitter import OAuth, Twitter, TwitterStream

# Initiate the connection to Twitter Streaming API
if __name__ == '__main__':
    oauth = OAuth(ConfigProvider.AccessToken, ConfigProvider.AccessSecret, ConfigProvider.ConsumerKey, ConfigProvider.ConsumberSecret)
    twitter_stream = TwitterStream(auth=oauth)
    twitter=Twitter(auth=oauth)
    LifeEventsList = FileContentLoader.LifeEventsList()
    AllTweets = None
    for LifeEvent in  LifeEventsList["topic_list"]:
        print "Processing tweets for life event:" , LifeEvent["topic"]
        EventTweets = twitter.search.tweets(q=LifeEvent["topic"], lang='en', count=10)
        for StemWord in LifeEvent["stem_words"]:
            EventStemWordTweets = twitter.search.tweets(q=StemWord, lang='en', count=5)
        EventTweets["statuses"] += EventStemWordTweets["statuses"]
        if AllTweets is None:
            AllTweets = EventTweets
        else:
            AllTweets["statuses"] += EventTweets["statuses"]
    
    AllUsers = {} 
    AllNonEventTweets = None
    for Status in AllTweets["statuses"]:
        print "Extracting user", Status["user"]["name"]
        if Status["user"]["id"] not in AllUsers:
            AllUsers[Status["user"]["id"]] = { "screen_name" : Status["user"]["screen_name"] , "name" : Status["user"]["name"] }
            if AllNonEventTweets is None:
                AllNonEventTweets = twitter.statuses.user_timeline(user_id = Status["user"]["id"], count = 5 )
            else:
                AllNonEventTweets += twitter.statuses.user_timeline(user_id = Status["user"]["id"], count = 5)
    AllTweets["statuses"] += AllNonEventTweets
    print "Writing tweets to disks"
    UserFile = open(ConfigProvider.UserFilePath,'w') 
    UserFile.write(json.dumps(AllUsers))
    UserFile.close()
    TweetsFile = open(ConfigProvider.TweetsFilePath,'w')
    TweetsFile.write(json.dumps(AllTweets["statuses"]))
    TweetsFile.close()
    