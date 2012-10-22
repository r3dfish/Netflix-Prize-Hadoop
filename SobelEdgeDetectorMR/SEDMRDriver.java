///////////////////////////////////////////////////////////////////////////////
//
//  Copyright (c) 2009, Arizona State University
//  All rights reserved.
//  BSD License: http://www.opensource.org/licenses/bsd-license.html
//  Author: Jeff Conner
//
///////////////////////////////////////////////////////////////////////////////

package SobelEdgeDetectorMR;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextOutputFormat;


public class SEDMRDriver {

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
			
			JobConf conf = new JobConf(SEDMRDriver.class);
			conf.setJobName("ImageProcessingMR");

			conf.setOutputKeyClass(Text.class);
			conf.setOutputValueClass(Text.class);

			conf.setMapperClass(SEDMRMapper.class);
			conf.setCombinerClass(SEDMRCombiner.class);
			conf.setReducerClass(SEDMRReducer.class);

			int size = args.length;

			if (size >= 2) {

				// Set the input and output formats
				conf.setInputFormat(OverlappingImageInputFormat.class);
				conf.setOutputFormat(TextOutputFormat.class);

				// set the input and output paths
				FileInputFormat.setInputPaths(conf, new Path(args[0]));
				FileOutputFormat.setOutputPath(conf, new Path(args[1]));	

				// run the job
				JobClient.runJob(conf);
			} 
			else 
			{
				System.out.println("Wrong number of arguements!\n" + printUsage());
			}

		}

	}
