FROM jupyter/pyspark-notebook

USER root

COPY . /home/jovyan/work

RUN /bin/bash -c 'chown -R jovyan:users /home/jovyan/'
