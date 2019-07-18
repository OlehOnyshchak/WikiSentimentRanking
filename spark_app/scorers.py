# from tools import  secure_import

# method may be 'vader', 'lr' or 'lr-advanced', spark is SparkSesision
def score_text(text, method='vader', pipelineModel=None, lrModel=None, spark=None):
    if pipelineModel is not None and lrModel is not None and spark is not None:
        if method == 'lr':
            return lr_scorer(text, lrModel, pipelineModel, spark)
        if method == 'lr-advanced':
            return lr_advanced_scorer(text, lrModel, pipelineModel, spark)
    else:
        return vader_scorer(text)
    
def vader_scorer(text):
    from nltk.sentiment.vader import SentimentIntensityAnalyzer
    sid = SentimentIntensityAnalyzer()
    score = sid.polarity_scores(text)
    return score["compound"]

def lr_scorer(text, lrModel, pipelineModel, spark):
    df = spark.createDataFrame([(text, 2)], ['text', 'target'])
    df_transformed = pipelineModel.transform(df) # To fix
    predictions = lrModel.transform(df_transformed)
    predictions = predictions.select(['text', 'probability', 'prediction'])
    pd_predictions = predictions.toPandas()
    positive_probability = pd_predictions.iloc[0]['probability'][1]
    overall_probability = 2 * positive_probability - 1   
    return overall_probability

def lr_advanced_scorer(text, lrModel, pipelineModel, spark):
    from numpy import mean as mean
    from nltk import tokenize
    sentences = tokenize.sent_tokenize(text)
    return mean([ lr_scorer(s, lrModel, pipelineModel, spark) for s in sentences])