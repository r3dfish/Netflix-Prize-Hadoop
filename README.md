This repository contains code to demonstrate Hadoop by attempting to solve the Netflix Prize.

Code by the Hacked Existence Team.
Project Page: http://hackedexistence.com/project-netflix.html

This code is meant to be run against the dataset provided by Netflix to teams who registered for the Netflix Prize Competition (http://www.netflixprize.com).  It is 2.1 Gigabytes in size broken down into individual files for each movie with a total of 17,770 movies in the data set.

Files:

netflixReorg.awk
This file is 3 lines of awk code that were used to reorganize the netflix prize dataset into something that would be much more efficient for use with hadoop.  It was used to take the first line of each file which contained the MovieID and append it to the beginning of each remaining line in the file so that Hadoop could be fed each line individually to compute against.




Netflix1
========

The Netflix1 program was written to produce statistical information about each movie in the dataset. It took the entire dataset as input and produced the first date rated, last date rated, total rating count and average rating for each movie in the dataset.



javaNetflix1/MyMapper.java
--------------------------
This file is the mapper written in Java for the Netflix1 project.  The output hashmap that gets sent to the reducer uses the MovieID as the key and the Rating,Date as the value (Rating and Date are combined as the value and separated by a comma which can then be parsed in the reducer).

pythonNetflix1/pyMapper.py
--------------------------
This file is the mapper for Netflix1 written in Python which is executed in the hadoop cloud by utilizing the streaming interface.  The output hashmap that gets sent to the reducer uses the MovieID as the key and Rating,Date as the value (Rating and Date are combined as the value and separated with a tab character '\t' which can be parsed in the reducer).

awkNetflix1/awkMapper.awk
-------------------------
This file is the mapper for Netflix1 written in Awk which is executed in the hadoop cloud by utilizing the streaming interface.  The output hashmap that gets sent to the reducer uses the MovieID as the key and Rating,Date as the value (Rating and Date are combined as the value and separated with a comma whcih can be parsed in the reducer).

javaNetflix1/MyReducer.java
---------------------------
This file is the reducer written in Java for the Netflix1 project.  It takes the output from MyMapper.java as input and computes the "First Date Rated, Last Date Rated, Production Date, Number of Ratings, Average Rating, Movie Title" for each movie in the data set.

pythonNetflix1/pyReducer.py
---------------------------
This file is the reducer writte in Python for the Netflix1 project.  It takes the output from pyMapper.py as input and computes the "First Date Rated, Last Date Rated, Production Date, Number of Ratings, Average Rating, Movie Title" for each movie in the data set.

awkNetflix1/pyReducer.py
------------------------
Because of the logic that had to be executed in the reducer, we were not able to write a reducer in Awk, and since the Awk mapper used the streaming interface, we chose to use the python reducer with the Awk mapper so that we could compare run times between the Python mapper and the Awk mapper using the same reducer.

javaNetflix1/netflix1Driver.java
--------------------------------
This is the Java driver file for the Netflix1 project, it registers the input data set in Distributed Cache, defines the mapper and reducer files/methods, and submits the job to the hadoop cloud for processing.

pythonNetflix1/pyNetflix1.sh
----------------------------
This is a shell script that calls the hadoop streaming command and sets all the job configuration as variables that it passes to the streaming command.

awkNetflix1/awkNetflix1.sh
--------------------------
This is a shell script that calls the hadoop streaming command and sets all the job configuration as variables that it passes to the streaming command.






Netflix2
========

The Netflix2 program was written to produce statistical information about each user in the dataset. It took the entire dataset as input and produced the total number of ratings, the overall average rating, and the rating delay (the number of days from movie production to date rated) for each user in the data set.





javaNetflix2/MyMapper.java
--------------------------
This file is the mapper written in Java for the Netflix2 project.  It takes the whole data set as input and outputs a hashmap that gets sent to the reducer using UserID as the key and "MovieID,Rating,RatingDelay" as the value.  It should also be noted that this mapper disregards any ratings that have NULL as the production date.

pythonNetflix2/pyMapper.py
--------------------------
This file is the mapper written in Python for the Netflix2 project.  It takes the whole data set as input and outputs a hashmap that gets sent to the reducer using UserID as the key and "MovieID,Rating,RatingDelay" as the value.  It should also be noted that this mapper disregards any ratings that have NULL as the production date, and reports the MovieID of disregarded movie to the job tracker.

javaNetflix2/MyReducer.java
---------------------------
This file is the reducer written in Java for the Netflix2 project.  It takes the output hashmap from javaNetflix2/MyMapper.java as input.  Its final output is UserID RatingCount,RatingAverage,RatingDelay.

pythonNetflix2/pyReducer.py
---------------------------
This file is the reducer written in Python for the Netflix2 project.  It takes the output hashmap from pythonNetflix2/pyMapper.py as input.  Its final output is UserID RatingCount,RatingAverage,RatingDelay.

javaNetflix2/netflix2Driver.java
--------------------------------
This is the Java driver file for the Netflix2 project, it registers the input data set in Distributed Cache, defines the mapper and reducer files/methods, and submits the job to the hadoop cloud for processing.

pythonNetflix2/pyNetflix2.sh
----------------------------
This is a shell script that calls the hadoop streaming command and sets all the job configuration as variables that it passes to the streaming command.
