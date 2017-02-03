#######################################################################################################################################
# File Name: Cleaner.py
# Author: Ashish Tyagi
# Date created: Jan 4, 2017
# Date last modified: Feb 2, 2017
# Python Version: 2.7
# Description: Cleans provided text string and replaces common symbols.   
#######################################################################################################################################

import re

# Removes unnecessary characters from a string 
def TextCleaner(RawString):
    # Convert the tweet to lower case
    RawString = RawString.lower() 
    # Remove new line characters
    RawString = re.sub("\n", " ", RawString)
    # remove multiple dots
    RawString = re.sub("\.\.+"," ", RawString)
    # Remove unicode emoji
    RawString = re.sub("\\\u....","", RawString)
    # Remove Url's
    RawString = re.sub("http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\(\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+", "" , RawString)
    # Remove multiple spaces
    RawString = re.sub("  +", " ", RawString)
    RawString = RawString.strip()
    return RawString

# # removes punctuation marks from text
def RemovePunctuations(Text):
    TextCleaned = ""
    TextCleaned = Text.replace(":","") \
                .replace(",","") \
                .replace("-"," ") \
                .replace("...","") \
                .replace("!","") \
                .replace("(","") \
                .replace(")","") \
                .replace(".","") \
                .replace("?","") \
                .replace("\"","") \
                .replace(";","")
    TextCleaned = TextCleaned.replace("  ", " ").strip()
    return TextCleaned

# Replaces Apostrophes word with equivalent words using Apostrophes word list 
def ReplaceApostrophes(ApostrophesReplaceList, Text):
    TextClaned = ""
    for Word in Text.split():
        if Word in ApostrophesReplaceList:
            TextClaned += " " + ApostrophesReplaceList[Word]
        else:
            TextClaned += " " + Word
    TextClaned = TextClaned.replace("  ", " ").strip()
    return TextClaned

# Removes stop words from text
def RemoveStopWords(StopWordList, Text):
    TextCleaned = ""
    for Word in Text.split(" "):
        if not Word in StopWordList:
            TextCleaned += " " + Word
    TextCleaned = TextCleaned.replace("  ", " ").strip()
    return TextCleaned