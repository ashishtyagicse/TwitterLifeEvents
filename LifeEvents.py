#######################################################################################################################################
# File Name: LifeEvents.py
# Author: Ashish Tyagi
# Date created: Jan 4, 2017
# Date last modified: Feb 2, 2017
# Python Version: 2.7
# Description: Starts a spark streaming application which can read tweets from a file and apply various classification models on it.  
#######################################################################################################################################


from utils import ConfigProvider, ContextProvider
from DataProvider import TwitterDataProvider

# Imports required model package and details for method to call  
def getModelImport(RunModels):
    ModelMethods = []
    for model in RunModels:
        try:
            importModule = __import__ ("ProcessingModels." + model + "Model")
            serviceModule = getattr(importModule, model + "Model")
            modelMethod = getattr(serviceModule, model + "Model")
            ModelMethods.append(modelMethod) 
        except ImportError:
            print 'No model found for service',
    return ModelMethods

# Runs a predefined model from Processing models on provided tweet's text
def runModel(DataRdd, ProcessModelMethods):
    for ModelMethod in ProcessModelMethods:
        ModelMethod(DataRdd)


if __name__ == '__main__':
    SparkStreamingContext = ContextProvider.getSparkStreamingContext()
    # Loads tweets from file and create RDD's 
    TweetsRdd = TwitterDataProvider.LoadTweetsFromFile()
    # Applies a model from defined processing models to these tweet rdd's 
    RunModels = ConfigProvider.RunModels
    ProcessModelMethods = getModelImport(RunModels)
    TweetsRdd.foreachRDD(lambda status: runModel( status, ProcessModelMethods))
    # Starts a spark streaming application 
    SparkStreamingContext.start()
    SparkStreamingContext.awaitTermination()
    