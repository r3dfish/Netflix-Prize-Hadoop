// MyReducer.java written for package netflix1reorg
// Written by Joey Calca
// r3dfish@hackedexistence.com
// http://hackedexistence.com

package netflix1reorg;

import java.io.IOException;
import java.io.BufferedReader;
import java.io.FileReader;
import java.util.HashMap;
import java.util.Iterator;
import java.util.StringTokenizer;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.StringUtils;

/**
   * A reducer class that outputs in the following format:
   * <movieID firstDateRated,lastDateRated,productionDate,numberOfRatings,averageRating,movieTitle>
   */
public class MyReducer{
  public static class Reduce extends MapReduceBase
    implements Reducer<Text, Text, Text, Text> {
    
    //path variable to hold path to files to be stored in Distributed Cache
	Path[] localFiles = new Path[0];
	
	//HashMap movieTitles to hold all the info from movie_titles.txt
	HashMap<String, String> movieTitles = new HashMap<String, String>();
  
    public void configure(JobConf job) {
        //if there are files for the Distributed Cache
        if(job.getBoolean("netflix1Driver.distributedCacheFile", false)) {
              //get the path for files stored in the Distributed Cache
              try {
                      localFiles = DistributedCache.getLocalCacheFiles(job);
                  }
                  catch (IOException ioe) {
                      System.err.println("Caught exception while getting cached files " + StringUtils.stringifyException(ioe));
                  }
                  //if there is a file in the distributed cache
                  if(localFiles[0].toString() != null) {
                      try {
                              //setup a BufferedReader for file stored at location localFiles[0]
                              //this program assumes only one file has been stored in Distributed Cache
                              //it is built to work with movie_titles.txt from the netflix prize dataset
                              BufferedReader reader = new BufferedReader(new FileReader(localFiles[0].toString()));
                              //string to hold each line from the cached file
                              String cachedLine = "";
                              //iterate through each line of the file stored in Distributed Cache
                              while ((cachedLine = reader.readLine()) != null) {
                                      //tokenize each line on ","
                                      StringTokenizer cachedIterator = new StringTokenizer(cachedLine, ",");
                                      //get the first token, the movieID
                                      String movieID = cachedIterator.nextToken();
                                      //get the rest of the tokens on the line
                                      String dateAndTitle = cachedIterator.nextToken();
                                      //while loop for movies that have a "," as part of their title
                                      while(cachedIterator.hasMoreTokens())
                                      {
                                    	  dateAndTitle += "," + cachedIterator.nextToken();
                                      }
                                      //store each line in the movieTitles HashMap
                                      //using the movieID as the key and the rest of the line as the value
                                      movieTitles.put(movieID, dateAndTitle);
                          }
                  } catch (IOException ioe) {
                          System.err.println("Caught Exception while parsing the cached file " + StringUtils.stringifyException(ioe));
                  }
              }
        }
    }
    

    public void reduce(Text key, Iterator<Text> values,
                       OutputCollector<Text, Text> output, 
                       Reporter reporter) throws IOException {

    	//int for oldest date and youngest date initialized to 0
    	//double rating will add up the values in rating then divide by ratingCount to get average rating
    	//int ratingCount will count the number of times a movie is rated
    	//string line to get value from mapper and remove "-" in date
    	int firstDate = 0;
    	int lastDate = 0;
    	double rating = 0.0;
    	int ratingCount = 0;
    	String line;
    	String dateStr = "";
    	
    	//iterate through the values from the mapper for each key
    	while(values.hasNext()) {
    		//convert the values to a string
    		line = values.next().toString();
    		//tokenize the string on ","
			StringTokenizer itr = new StringTokenizer(line, ",");
			//add the ratings for each movie up
			rating += Integer.parseInt(itr.nextToken());
    		//get the date value and remove "-"
			dateStr = itr.nextToken();
    		dateStr = dateStr.replaceAll("-","");
    		//if this is the first date in the value list
    		if(firstDate == 0) {
    			firstDate = Integer.parseInt(dateStr);
    			lastDate = firstDate;
    			//initial ratingCount increment
    			ratingCount++;
    		}
    		//is current date newer than lastDate
    		if(Integer.parseInt(dateStr) > lastDate) {
    			lastDate = Integer.parseInt(dateStr);
    		}
    		//is current date older than firstDate
    		if(Integer.parseInt(dateStr) < firstDate) {
    			firstDate = Integer.parseInt(dateStr);
    		}
    		//increment ratingCount
    		ratingCount++;
    	  }//end while loop
    	
    	//get the movie information from the HashMap
    	String movieInfo = movieTitles.get(key.toString());
    	//tokenize the movieInfo on ","
    	StringTokenizer tokenizer = new StringTokenizer(movieInfo, ",");
    	//get the movie production date from the HashMap value
    	String prodDate = tokenizer.nextToken();
    	//get the movie title from the HashMap value
    	String movieTitle = tokenizer.nextToken();
    	//while loop to get the rest of the title even if it has a "," in it
    	while(tokenizer.hasMoreTokens())
    	{
    		movieTitle += "," + tokenizer.nextToken();
    	}
    	
    	//calculate the average rating for each movie
    	rating = rating/ratingCount;
    	//string dateRange to hold the output value
    	//add the first and last date rated to the dateRange
		String dateRange = Integer.toString(firstDate) + "," + Integer.toString(lastDate);
		//add the date of production of the movie to dateRange
		dateRange += "," + prodDate;
		//add the total rating count for each movie to dateRange
		dateRange += "," + ratingCount;
		//add the average rating for each movie to dateRange
		dateRange += "," + rating;
		//add the movie title to dateRange
		dateRange += "," + movieTitle;
		//convert dateRange to a Text for output value
		Text dateRangeText = new Text(dateRange);
		//output <movieID firstDateRated,lastDateRated,productionDate,numberOfRatings,averageRating,movieTitle>
		output.collect(key, dateRangeText);
    	}
    }
  }
