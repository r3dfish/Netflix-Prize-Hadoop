///////////////////////////////////////////////////////////////////////////////
//
//  Copyright (c) 2009, Arizona State University
//  All rights reserved.
//  BSD License: http://www.opensource.org/licenses/bsd-license.html
//  Author: Jeff Conner
//
///////////////////////////////////////////////////////////////////////////////

package SobelEdgeDetectorMR;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class SEDMRCombiner extends MapReduceBase implements Reducer<Text, Text, Text, Text> 
{
	// /////////////////////////////////////////////////////////////////////////////
	//
	// Combiner
	//
	// /////////////////////////////////////////////////////////////////////////////

	public void reduce(Text key, Iterator<Text> values,
			OutputCollector<Text, Text> output, Reporter reporter)
			throws IOException 
	{
		// collect and pass on the output
		while( values.hasNext() )
		{
			output.collect( key, values.next() );
		}
	}
}