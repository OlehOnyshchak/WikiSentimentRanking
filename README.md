# WikiSentimentRanking

## Project Report

The [Project Report](https://github.com/OlehOnyshchak/WikiSentimentRanking/blob/master/Project%20Report.ipynb) contains: 
- *Problem Statement*,
- *Motivation*, 
- *Problem Formulation* (in context of Machine Learning), 
- Description of the *Approach to Solution*, 
- Utilized *Data*, 
- *Evaluation* process, 
- *Results & Discussion*.

## Main Notebook

[main.ipynb](https://github.com/OlehOnyshchak/WikiSentimentRanking/blob/master/main.ipynb)


### Build instructions

You can run the Shell script `./start_env.sh` or configure Docker manually:

```
$ docker pull jupyter/pyspark-notebook
$ docker build -t wiki_sentiment_ranking .
$ docker run -it --rm -p 8888:8888 wiki_sentiment_ranking
```

### Evaluation

The two algorithms were evaluated in the next notebooks:
- [Advanced_LR_Evaluation.ipynb](https://github.com/OlehOnyshchak/WikiSentimentRanking/blob/master/evaluation/Advanced_LR_Evaluation.ipynb)
- [VADER_Evaluation.ipynb](https://github.com/OlehOnyshchak/WikiSentimentRanking/blob/master/evaluation/VADER_Evaluation.ipynb)
