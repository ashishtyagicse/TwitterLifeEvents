import re

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

def RemovePunctuations(Text):
    TextClaned = ""
    TextClaned = Text.replace(":","") \
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
    TextClaned = TextClaned.replace("  ", " ").strip()
    return TextClaned
    
def ReplaceApostropes(ApostropesReplaceList, Text):
    TextClaned = ""
    for Word in Text.split():
        if Word in ApostropesReplaceList:
            TextClaned += " " + ApostropesReplaceList[Word]
        else:
            TextClaned += " " + Word
    TextClaned = TextClaned.replace("  ", " ").strip()
    return TextClaned

def RemoveStopWords(StopWordList, Text):
    TextClaned = ""
    for Word in Text.split(" "):
        if not Word in StopWordList:
            TextClaned += " " + Word
    TextClaned = TextClaned.replace("  ", " ").strip()
    return TextClaned