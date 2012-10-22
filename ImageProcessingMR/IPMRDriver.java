///////////////////////////////////////////////////////////////////////////////
//
//  Copyright (c) 2009, Arizona State University
//  All rights reserved.
//  BSD License: http://www.opensource.org/licenses/bsd-license.html
//  Author: Jeff Conner
//
///////////////////////////////////////////////////////////////////////////////

package ImageProcessingMR;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextOutputFormat;


public class IPMRDriver {

	// /////////////////////////////////////////////////////////////////////////////
	//
	// Print usage
	//
	// /////////////////////////////////////////////////////////////////////////////

	public static String printUsage() 
	{
		String message = new String("");

		return message;
	}

	// /////////////////////////////////////////////////////////////////////////////
	//
	// Main
	//
	// /////////////////////////////////////////////////////////////////////////////

	public static void main(String[] args) throws Exception 
	{
		// debug
		System.out.println( "main called in IPMRDriver" );
		
		JobConf conf = new JobConf(IPMRDriver.class);
		conf.setJobName("ImageProcessingMR");

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(BytesWritable.class);

		conf.setMapperClass(IPMRMapper.class);
		conf.setCombinerClass(IPMRCombiner.class);
		conf.setReducerClass(IPMRReducer.class);

		int size = args.length;

		if (size >= 2) {

			// Set the input and output formats
			conf.setInputFormat(ImageInputFormat.class);
			conf.setOutputFormat(TextOutputFormat.class);

			// debug
			System.out.println( "Before input/output path sets" );
			
			// set teh input and output paths
			FileInputFormat.setInputPaths(conf, new Path(args[0]));
			FileOutputFormat.setOutputPath(conf, new Path(args[1]));	

			// debug
			System.out.println( "After i/o sets, before runJob" );
			
			// run the job
			JobClient.runJob(conf);
		} 
		else 
		{
			System.out.println("Wrong number of arguements!\n" + printUsage());
		}

	}

}
