from pyspark.ml.classification import LogisticRegressionModel
from pyspark.ml import PipelineModel

def readFromCache():
    lrModel = LogisticRegressionModel.load("./train/lr.model")
    pipelineModel = PipelineModel.load('./train/pipeline.model')
    print('models loaded')
    
    return (lrModel, pipelineModel)