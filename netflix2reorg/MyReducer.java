// MyReducer.java for package netflix2reorg
// Written by Joey Calca
// r3dfish@hackedexistence.com
// http://hackedexistence.com

// MyReducer.java written for package netflix2reorg
// Written by Joey Calca
// r3dfish@gmail.com

package netflix2reorg;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

/**
   * A reducer class that outputs in the following format:
   * <UserID totalNumRatings,avgRating,ratingDelay>
   */

public class MyReducer{
  public static class Reduce extends MapReduceBase
    implements Reducer<Text, Text, Text, Text> {
	  
    public void reduce(Text key, Iterator<Text> values,
                       OutputCollector<Text, Text> output, 
                       Reporter reporter) throws IOException {
    	
    	//double ratingTotal to hold the sum of the ratings for all movies viewed by a single user
    	//int ratingCount will count the number of movies a user rates
    	//string line to get value from mapper and remove "-" in date
    	//double ratingDelay to calculate the average rating delay for each user
    	
    	Double ratingTotal = 0.0;
    	int ratingCount = 0;
    	String line;
    	Double ratingDelay = 0.0;
  	
    	//iterate through each <K V> output from the mapper
    	while(values.hasNext()) {
    		//iterate through each value passed from the mapper
    		line = values.next().toString();
    		//tokenize input on ","
			StringTokenizer itr = new StringTokenizer(line, ",");
			//skip the movieID
			itr.nextToken();
			//add up the rating for each movie
			ratingTotal += Integer.parseInt(itr.nextToken().toString());
    		//increment ratingCount
    		ratingCount++;
    		//add up the rating delay for each movie
    		ratingDelay += Integer.parseInt(itr.nextToken().toString());
    	  }//end while loop
      
	    //ratingAvg is computed by dividing the total of ratings by the number of ratings
	    //stored in a Double to get rating to a decimal, netflix stores as an int
    	Double ratingAvg = Double.valueOf(ratingTotal.toString());
    	ratingAvg = ratingAvg/ratingCount;

    	//compute ratingDelay across all movies rated by a user
    	ratingDelay = ratingDelay / ratingCount;
    	
    	//string for value for output <KV> pairs
       	String dateRange = "";
   		dateRange += ratingCount;
   		dateRange += "," + ratingAvg;
   		dateRange += "," + ratingDelay;

   		Text dateRangeText = new Text(dateRange);
   		//output <userID ratingCount,ratingAverage,ratingDelay>
  		output.collect(key, dateRangeText);
    	}
    }
  }
