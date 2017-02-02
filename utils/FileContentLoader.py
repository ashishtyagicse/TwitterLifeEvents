import json
from utils import ConfigProvider

def LoadStopWords():
    with open(ConfigProvider.StopWordsPath) as f:
        StopWordsList = f.readlines()
        StopWordsList = [word.replace("\n","") for word in StopWordsList]
    return StopWordsList

def LoadApostropesReplaceWords():
    ApostropesWordList = json.load(open(ConfigProvider.ApostropesWordPath))
    return ApostropesWordList

def LifeEventsList():
    TopicJsonList = json.load(open(ConfigProvider.LifeEventsListPath))
    return TopicJsonList