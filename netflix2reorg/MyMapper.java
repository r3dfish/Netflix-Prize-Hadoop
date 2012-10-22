// MyMapper.java written for package netflix2reorg
// Written by Joey Calca
// r3dfish@hackedexistence.com
// http://hackedexistence.com

package netflix2reorg;

import java.io.*;
import java.util.*;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.StringUtils;

public class MyMapper {
	
	  /**
	   * Mapper takes input from the reorganized NetFlix Prize Dataset.  It iterates
	   * through each file in the training_set_reorg.  Each line is in the form
	   * MovieID,UserID,Rating,Date.  The output of Mapper's <key value> is
	   * <UserID MovieID,Rating,RatingDelay>
	   */

	
	  public static class MapClass extends MapReduceBase
	    implements Mapper<LongWritable, Text, Text, Text> {
		  
		  //path variable to hold path to files stored in Distributed Cache
		  //hashMap dateMap to hold a hashMap of values pulled out of movie_titles.txt from Distributed Cache
		  //dateMap key is movieID and value is movie production date
		  Path[] localFiles = new Path[0];
		  HashMap<String, String> dateMap = new HashMap<String, String>();
		    
		  public void configure(JobConf job) {
			    //if there are files for the Distributed Cache
			    	if(job.getBoolean("netflix2reorgDriver.distributedCacheFile", false)) {
			    		//get the path for files to be stored in the Distributed Cache
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
			  	    		  String cachedLine = "";
			  	    		  //iterate through each line of the file stored in Distributed Cache
			  	    		  while ((cachedLine = reader.readLine()) != null) {
			  	    			  //tokenize each line on ","
			  	    			  StringTokenizer cachedIterator = new StringTokenizer(cachedLine, ",");
			  	    			  //string cachedMovieID to pull the movieID from movie_titles.txt
			  	    	    	  //string productionDate to pull values from movie_titles.txt
			  	    	    	  //movie_titles.txt is stored in Distributed Cache
			  	    			  String cachedMovieID = "";
			  	    			  String productionDate = "";
			  	    			  cachedMovieID = cachedIterator.nextToken();
			  	    			  productionDate = cachedIterator.nextToken();
			  	    			  //use the MovieID as the Key for the hashMap
			  	    			  //use productionDate as the Value for the hashMap
			  	    			  dateMap.put(cachedMovieID, productionDate);
			  	    		  }
			  	    	  } catch (IOException ioe) {
			  	    		  System.err.println("Caught Exception while parsing the cached file " + StringUtils.stringifyException(ioe));
			  	    	  }
			  	      }
			    	}
			    }  
	    
		//text object word to hold the Key for each <KV> pair
	    private Text word = new Text();  
	    
	    public void map(LongWritable key, Text value, 
	                    OutputCollector<Text, Text> output, 
	                    Reporter reporter) throws IOException {
	      //convert Text value to String
	      String line = value.toString();
	      //movie ratings are in the form "MovieID,UserID,Rating,Date"
	      //each separate rating is delimited by a line break
	      //tokenize the strings on ","
	      StringTokenizer itr = new StringTokenizer(line, ",");
	      //get the movieID from the tokenizer
	      String movieID = itr.nextToken();
	      //get the userID from the tokenizer
	      String userID = itr.nextToken();
	      //use it as the key in the <KV> output pair
	      word.set(userID);
	      //get the rating for the user and movie pair
	      String rating = itr.nextToken();
	      //get the date rated for the <user movie rating>
	      String dateRated = itr.nextToken();
  		  dateRated = dateRated.replaceAll("-","");
	      //string to hold the production date from the dateMap
  		  String productionDate = dateMap.get(movieID);
  		
      	//If you work at netflix, you should add something to the readme.txt file about
      	//the following anomalies found in the movie_titles.txt file
/**    	
*      	[jcalca@h0-2 ~]$ grep NULL movie_titles.txt 
*      	4388,NULL,Ancient Civilizations: Rome and Pompeii
*      	4794,NULL,Ancient Civilizations: Land of the Pharaohs
*      	7241,NULL,Ancient Civilizations: Athens and Greece
*      	10782,NULL,Roti Kapada Aur Makaan
*      	15918,NULL,Hote Hote Pyaar Ho Gaya
*      	16678,NULL,Jimmy Hollywood
*      	17667,NULL,Eros Dance Dhamaka
*/
      	//use a try catch block in order to catch the NULL movie production dates
      	//pulled out of the movie_titles.txt and discard those movies
      	//optionally, you could add the proper values into the hashMap manually
      	//after parsing, or add the proper values to the input txt file
      	try{
      	//compute the delay between movie production date and user rating date
      	//store the difference in ratingDelay
    		int prodDate = Integer.parseInt(productionDate);
     		int ratedDate = Integer.parseInt(dateRated.substring(0,4));
     		int ratingDelay = ratedDate - prodDate;
  	        //string to hold the value for the <KV> output pair
  	        String outputStr = movieID;
        	outputStr += "," + rating;
        	outputStr += "," + ratingDelay;
        	//output <userID movieID,rating,ratingDealy> to the reducer
  	      output.collect(word, new Text(outputStr));
      	}
 		//catch NumberFormatException and return void
 		//this will skip collecting the output for movies with "NULL" production dates
      	catch (NumberFormatException nfe) {
      		System.err.println("Caught NumberFormat Exception: " + StringUtils.stringifyException(nfe));
      	}
	    }
	  }
}
