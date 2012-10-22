///////////////////////////////////////////////////////////////////////////////
//
//  Copyright (c) 2009, Arizona State University
//  All rights reserved.
//  BSD License: http://www.opensource.org/licenses/bsd-license.html
//  Author: Jeff Conner
//
///////////////////////////////////////////////////////////////////////////////

package ImageProcessingMR;

import java.io.*;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobConfigurable;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.FileSplit;

public class ImageInputFormat extends FileInputFormat<Text, BytesWritable>
implements JobConfigurable 
{

	@Override
	public RecordReader<Text, BytesWritable> getRecordReader
			( InputSplit genericSplit, JobConf job, Reporter reporter) throws IOException 
    {
		// debug
		System.out.println( "Constructor called in ImageInputFormat" );
		
		reporter.setStatus( genericSplit.toString() );
		return new ImageRecordReader( job, ( FileSplit ) genericSplit );
    }

	@Override
	public void configure(JobConf arg0) 
	{
		// debug
		System.out.println( "configure called in ImageInputFormat" );
		
	}
	
	protected boolean isSplitable(FileSystem fs, Path file) 
	{
		// debug
		System.out.println( "isSplitable called in ImageInputFormat" );
	    return false;
	}

}
