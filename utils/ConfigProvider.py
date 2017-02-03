#######################################################################################################################################
# File Name: ConfigProvider.py
# Author: Ashish Tyagi
# Date created: Jan 4, 2017
# Date last modified: Feb 2, 2017
# Python Version: 2.7
# Description: reads application.conf file and loads values for various configuration parameters   
#######################################################################################################################################

import ConfigParser
# Reads application.conf file
config = ConfigParser.SafeConfigParser()
config.read("resources/application.conf")

# Provides paths for varous resources 
TweetsFilePath = config.get("Paths", "TweetsFile")
ApostrophesWordPath = config.get("Paths", "ApostrophesWord")
StopWordsPath = config.get("Paths", "StopWords")
LifeEventsListPath = config.get("Paths", "LifeEventsList")
UserFilePath = config.get("Paths", "UserFile")
OutputParquetFilePath = config.get("Paths", "OutoutParquetFile")

# Provides job related details and hostnames
JobName = config.get("Job Details", "JobName")
# Used for local mode
#ExecutionMode = config.get("Job Details","ExecutionMode")
StreamingInterval = config.get("Job Details","StreamingInterval")
HbaseHost = config.get("Job Details","HbaseHost")
MaxPartFiles = config.get("Job Details","MaxPartFiles")

# Provides details of all available models and models that needs to be run
AllModels = config.get("Models", "all").split(',')
RunModels = config.get("Models", "run").split(',')

# Twitter data connection parameter details
AccessToken = config.get("Twitter Credentials" , "AccessToken")
AccessSecret = config.get("Twitter Credentials" , "AccessSecret")
ConsumerKey = config.get("Twitter Credentials" , "ConsumerKey")
ConsumberSecret = config.get("Twitter Credentials" , "ConsumberSecret")