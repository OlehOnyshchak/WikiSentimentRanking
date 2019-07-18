FROM jupyter/pyspark-notebook

USER root

COPY . /home/jovyan/work

WORKDIR /home/jovyan/work

RUN /bin/bash -c 'chown -R jovyan:users /home/jovyan/'

RUN pip install -r requirements.txt

RUN python -c "import nltk; nltk.downloader.download('vader_lexicon')"