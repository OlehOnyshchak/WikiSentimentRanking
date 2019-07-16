#!/bin/bash

docker pull jupyter/pyspark-notebook

docker build -t wiki_sentiment_ranking .

docker run -it --rm -p 8888:8888 wiki_sentiment_ranking
