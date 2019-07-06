def score_text(text):
    from nltk.sentiment.vader import SentimentIntensityAnalyzer
    sid = SentimentIntensityAnalyzer()
    score = sid.polarity_scores(text)
    return score["compound"]