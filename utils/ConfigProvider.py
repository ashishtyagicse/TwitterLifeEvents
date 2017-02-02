import ConfigParser

config = ConfigParser.SafeConfigParser()
config.read("resources/application.conf")

TweetsFilePath = config.get("Paths", "TweetsFile")
ApostropesWordPath = config.get("Paths", "ApostropesWord")
StopWordsPath = config.get("Paths", "StopWords")
LifeEventsListPath = config.get("Paths", "LifeEventsList")
UserFilePath = config.get("Paths", "UserFilePath")

JobName = config.get("Job Details", "JobName")
ExecutionMode = config.get("Job Details","ExecutionMode")
StreamingInterval = config.get("Job Details","StreamingInterval")
HbaseHost = config.get("Job Details","HbaseHost")

AllModels = config.get("Models", "all").split(',')
RunModels = config.get("Models", "run").split(',')

AccessToken = config.get("Twitter Credentials" , "AccessToken")
AccessSecret = config.get("Twitter Credentials" , "AccessSecret")
ConsumerKey = config.get("Twitter Credentials" , "ConsumerKey")
ConsumberSecret = config.get("Twitter Credentials" , "ConsumberSecret")