#######################################################################################################################################
# File Name: FileContentLoader.py
# Author: Ashish Tyagi
# Date created: Jan 4, 2017
# Date last modified: Feb 2, 2017
# Python Version: 2.7
# Description: loads the content of requested file from filesystem and returns it to calling function   
#######################################################################################################################################

import json
from utils import ConfigProvider

# Loads stop word list and returns a list of words
def LoadStopWords():
    with open(ConfigProvider.StopWordsPath) as f:
        StopWordsList = f.readlines()
        StopWordsList = [word.replace("\n","") for word in StopWordsList]
    return StopWordsList

# Loads Apostrophes replacement words in form of a Json structure
def LoadApostrophesReplaceWords():
    ApostrophesWordList = json.load(open(ConfigProvider.ApostrophesWordPath))
    return ApostrophesWordList

# Loads list of life events in Json form 
def LifeEventsList():
    EventJsonList = json.load(open(ConfigProvider.LifeEventsListPath))
    return EventJsonList