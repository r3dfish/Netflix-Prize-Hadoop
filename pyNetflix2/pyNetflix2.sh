#! /bin/bash

#################################
# pNetflix2.sh                  #
# pyNetflix2                    #
#                               #
# by Ryan Anguiano              #
# bl4ckbird@hackedexistence.com #
#################################

# 
# Calls the Hadoop Streaming command (assuming you're running 0.19.0) and
# passes the following variables:
#
# input:	Input dataset
# output:	Output location
# mapper:	Mapper to be used (Any executable located or available on the mapper)
# reducer:	Reducer to be used (See above)
# file:		Files to be included in the jar sent to each mapper and reducer
#			(It is generally a good idea to include the mapper and reducer
#			unless you have another predetermined method of accessing those
# 			files)
# cacheFile:Files already uploaded to HDFS that will be loaded into cache
# 			on each node. Generally a good idea to add a file here if
#			you will be accessing this file many times to avoid Disk I/O
#			penalties.
# jobconf:	Hadoop variables (See Hadoop Docs)
#

hadoop jar $HADOOP_HOME/contrib/streaming/hadoop-0.19.0-streaming.jar \
-input /datasets/Netflix-dataset/training_set_reorg/* \
-output pyNetflix2Output \
-mapper pyMapper.py \
-reducer pyReducer.py \
-file /home/ranguiano/workspace/pyNetflix2/pyMapper.py \
-file /home/ranguiano/workspace/pyNetflix2/pyReducer.py \
-cacheFile 'hdfs://s49-1.local:9001/datasets/Netflix-dataset/movie_titles.txt#movie_titles.txt' \
-jobconf mapred.job.name='pyNetflix2'
