///////////////////////////////////////////////////////////////////////////////
//
//  Copyright (c) 2009, Arizona State University
//  All rights reserved.
//  BSD License: http://www.opensource.org/licenses/bsd-license.html
//  Author: Jeff Conner
//
///////////////////////////////////////////////////////////////////////////////

package SobelEdgeDetectorMR;

import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.StringTokenizer;

import javax.imageio.ImageIO;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;


public class SEDMRMapper extends MapReduceBase implements Mapper<Text, BytesWritable, Text, Text> 
{
	// /////////////////////////////////////////////////////////////////////////////
	//
	// Member variables
	//
	// /////////////////////////////////////////////////////////////////////////////

	private String _filename;
	
	// /////////////////////////////////////////////////////////////////////////////
	//
	// Map
	//
	// /////////////////////////////////////////////////////////////////////////////

	public void map(Text key, BytesWritable value,
			OutputCollector<Text, Text> output, Reporter reporter)
			throws IOException 
	{
		// Grab a value
		String keyAsString = key.toString();
		
		// debugging
		System.out.println( "Key in Mapper: " + key );
		
		// tokenize the value so we can iterate through the strings
	    StringTokenizer itr = new StringTokenizer( keyAsString );
	    
	    // set the filename
	    _filename = itr.nextToken();
	    
	    // set the starting x
	    String x = itr.nextToken();
	    
	    // set the starting y
	    String y = itr.nextToken();
	    
	    // set the width
	    String w = itr.nextToken();
	    
	    // set the height
	    String h = itr.nextToken();
	    
	    // set the width step size
	    String wStep = itr.nextToken();
	    
	    // set the height step size
	    String hStep = itr.nextToken();
	    
	   	// create an input stream to parse the value data
		ByteArrayInputStream bis = new ByteArrayInputStream( value.getBytes(), 0, value.getLength() );		
				
		// read the image from the input stream
		BufferedImage image = ImageIO.read( bis );
		
		// create a Sobel Edge detector object
		SobelEdgeDetector detector = new SobelEdgeDetector( image );
		
		// process the image
		detector.process();
		
		// get the image
		image = detector.results();		
		
		// get the directory and basename
		String basename = UsulFilePath.base( _filename.toString() );
		String directory = UsulFilePath.directory( _filename.toString() );		
		
		// create the output file name
		String filename = new String( directory + basename + "_output/" + basename + "_sobel-x_" + x + "-y_" + y + "-w_" + w + "-h_" + h + ".jpg" );
		
		// Create a configuration object
		Configuration config = new Configuration();

		// Create a filesystem object
		FileSystem hdfs = FileSystem.get(config);		
		
		// create a path object from the filename
		Path path = new Path( filename );
		
		// set the bytes to write
		ByteArrayOutputStream bos = new ByteArrayOutputStream(1000);
		ImageIO.write(image, "jpg", bos);
		byte[] byteImage = bos.toByteArray();		
		
		// Create an output stream
		FSDataOutputStream os = hdfs.create( path );
		
		// write the file
		os.write(byteImage, 0, byteImage.length);

		// close the stream
		os.close();
						
		// set the result
		String result = new String( filename + " " + x + " " + y + " " + w + " " + h + " " + wStep + " " + hStep );
			
		// output to the combiner
		output.collect( new Text( _filename ), new Text( result ) );
		
	}
}

