FROM jupyter/pyspark-notebook

USER root

COPY . /home/jovyan/work

RUN /bin/bash -c 'chmod -R 777  /home/jovyan/'
