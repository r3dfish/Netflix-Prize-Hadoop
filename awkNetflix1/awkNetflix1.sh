#! /bin/bash
hadoop jar $HADOOP_HOME/contrib/streaming/hadoop-0.19.0-streaming.jar \
-input /datasets/Netflix-dataset/training_set_reorg/* \
-output awkNetflix1Output \
-mapper "awk -f awkMapper.awk" \
-reducer pyReducer.py \
-file /home/ranguiano/workspace/awkNetflix1/awkMapper.awk \
-file /home/ranguiano/workspace/awkNetflix1/pyReducer.py \
-cacheFile 'hdfs://s49-1.local:9001/datasets/Netflix-dataset/movie_titles.txt#movie_titles.txt' \
-jobconf mapred.job.name='awkNetflix1'
