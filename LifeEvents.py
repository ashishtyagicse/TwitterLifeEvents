import json
from utils import ConfigProvider, ContextProvider
from DataProvider import TwitterDataProvider

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
def test(rdd):
    print rdd.collect()

def runModel(DataRdd, ProcessModelMethods):
    for ModelMethod in ProcessModelMethods:
        ModelMethod(DataRdd)

if __name__ == '__main__':
    SparkStreamingContext = ContextProvider.getSparkStreamingContext()
    TweetsRdd = TwitterDataProvider.LoadTweetsFromFile()
    RunModels = ConfigProvider.RunModels
    ProcessModelMethods = getModelImport(RunModels)
    #TweetsRdd.foreachRDD(lambda status: test( status))
    TweetsRdd.foreachRDD(lambda status: runModel( status, ProcessModelMethods))
    SparkStreamingContext.start()
    SparkStreamingContext.awaitTermination()
    