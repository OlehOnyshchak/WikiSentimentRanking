
# method may be 'vader', 'lr' or 'lr-advanced'
def score_text(text, method='vader', pipelineModel=None, lrModel=None):
    if method == 'lr-advanced' and pipelineModel is not None and lrModel is not None:
        return lr_advanced_scorer(text, lrModel, pipelineModel)
    else:
        return vader_scorer(text)
    
def vader_scorer(text):
    from nltk.sentiment.vader import SentimentIntensityAnalyzer
    sid = SentimentIntensityAnalyzer()
    score = sid.polarity_scores(text)
    return score["compound"]

# TO DO: fix SparkSession varible `spark`
def lr_scorer(text, lrModel, pipelineModel):
    df = spark.createDataFrame([(text, 2)], ['text', 'target'])
    df_transformed = pipelineModel.transform(df) # To fix
    predictions = lrModel.transform(df_transformed)
    predictions = predictions.select(['text', 'probability', 'prediction'])
    pd_predictions = predictions.toPandas()
    positive_probability = pd_predictions.iloc[0]['probability'][1]
    overall_probability = 2 * positive_probability - 1   
    return overall_probability

def lr_advanced_scorer(text, lrModel, pipelineModel):
    from nltk import tokenize
    sentences = tokenize.sent_tokenize(text)
    return np.mean([ lr_scorer(s, lrModel, pipelineModel) for s in sentences])