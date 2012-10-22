// MyMapper.java written for package netflix1reorg
// Written by Joey Calca
// r3dfish@hackedexistence.com
// http://hackedexistence.com

package netflix1reorg;

import java.io.*;
import java.util.*;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

public class MyMapper {
	  /**
	   * Mapper takes input from the reorganized NetFlix Prize Dataset.  It iterates
	   * through each file in the training_set_reorg folder.  Each line is in the form
	   * of MovieID,UserID,Rating,Date.  The output of Mappers <key value> is
	   * <MovieID Rating,Date>
	   */
	  public static class MapClass extends MapReduceBase
	    implements Mapper<LongWritable, Text, Text, Text> {
	    
	    private Text word = new Text();  
	    public void map(LongWritable key, Text value, 
	                    OutputCollector<Text, Text> output, 
	                    Reporter reporter) throws IOException {
	      //convert Text value to string
	      String line = value.toString();
	      //movie ratings are in the form "movieID,userID,rating,date"
	      //each seperate <movieID,userID,rating,date> is delimited by a line break
	      //tokenize the strings on ","
	      StringTokenizer itr = new StringTokenizer(line, ",");
	      //String name to hold the movieID
	      String name = itr.nextToken();
	      //set the movieID as the Key for the output <K V> pair
	      word.set(name);
	      //string to hold rating and date for each movie
	      String ratingAndDate = "";
	      //skip the userID
	      itr.nextToken();
	      //get the rating
	      ratingAndDate = itr.nextToken();
	      //get the date
	      ratingAndDate += "," + itr.nextToken();
	      //output the <movieID rating,date> to the reducer
	      output.collect(word, new Text(ratingAndDate));
	    }
	  }
}
