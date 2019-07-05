from pyspark.ml.classification import LogisticRegressionModel
from pyspark.ml import PipelineModel

def readFromCache(train_path):
    lrModel = LogisticRegressionModel.load(train_path + "lr.model")
    pipelineModel = PipelineModel.load(train_path + 'pipeline.model')
    print('models loaded')
    
    return (lrModel, pipelineModel)
