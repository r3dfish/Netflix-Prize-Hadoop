// netflix2reorgDriver written for the netflix2reorg package
// Written by Joey Calca
// r3dfish@hackedexistence.com
// http://hackedexistence.com

package netflix2reorg;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import netflix2reorg.MyMapper;
import netflix2reorg.MyReducer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.filecache.DistributedCache;

public class netflix2reorgDriver extends Configured implements Tool {


	/**
	 * This is a Hadoop Map/Reduce application.
	 * It reads the text input files, parses each line to a string, pulls out the
	 * movie rating and date for each user. The output is a <key value> list, where key
	 * is the UserID and value is a comma separated list of 
	 * FirstRatingDate,LastRatingDate,RatingCount,AverageRating.
	 * This will give us range of dates that a movie has been rated over, the total
	 * number of times a movie has been rated, and the average rating for each movie
	 * for each unique user.
	 * 
	 * The intended input set for this Map/Reduce application is the training_set_reorg
	 * folder from the netflix prize dataset.  It can be downloaded from
	 * http://www.netflixprize.com
	 */
	  
	  
	  static int printUsage() {
	    System.out.println("netflix1Driver [-m <maps>] [-r <reduces>] [- d <distributedCache>] <input> <output>");
	    ToolRunner.printGenericCommandUsage(System.out);
	    return -1;
	  }
	  
	  /**
	   * The main driver for netflix2reorg map/reduce program.
	   * Invoke this method to submit the map/reduce job.
	   * @throws IOException When there is communication problems with the 
	   *                     job tracker.
	   */
	  public int run(String[] args) throws Exception {
	    JobConf conf = new JobConf(getConf(), MyMapper.class);
	    conf.setJobName("netflix2reorgDriver");
	 
	    conf.setOutputKeyClass(Text.class);
	    conf.setOutputValueClass(Text.class);
	    conf.setMapperClass(MyMapper.MapClass.class);        
	    conf.setReducerClass(MyReducer.Reduce.class);
	    
	    List<String> other_args = new ArrayList<String>();
	    for(int i=0; i < args.length; ++i) {
	      try {
	        if ("-m".equals(args[i])) {
	          conf.setNumMapTasks(Integer.parseInt(args[++i]));
	        } else if ("-r".equals(args[i])) {
	          conf.setNumReduceTasks(Integer.parseInt(args[++i]));
	        } else if ("-d".equals(args[i])) {
	        	DistributedCache.addCacheFile(new Path(args[++i]).toUri(), conf);
	        	conf.setBoolean("netflix2reorgDriver.distributedCacheFile", true);
	        } else {
	          other_args.add(args[i]);
	        }
	      } catch (NumberFormatException except) {
	        System.out.println("ERROR: Integer expected instead of " + args[i]);
	        return printUsage();
	      } catch (ArrayIndexOutOfBoundsException except) {
	        System.out.println("ERROR: Required parameter missing from " +
	                           args[i-1]);
	        return printUsage();
	      }
	    }
	    // Make sure there are exactly 2 parameters left.
	    if (other_args.size() != 2) {
	      System.out.println("ERROR: Wrong number of parameters: " +
	                         other_args.size() + " instead of 2.");
	      return printUsage();
	    }
	    FileInputFormat.setInputPaths(conf, other_args.get(0));
	    FileOutputFormat.setOutputPath(conf, new Path(other_args.get(1)));
	        
	    JobClient.runJob(conf);
	    return 0;
	  }
	  
	  
	  public static void main(String[] args) throws Exception {
	    int res = ToolRunner.run(new Configuration(), new netflix2reorgDriver(), args);
	    System.exit(res);
	  }

	}



