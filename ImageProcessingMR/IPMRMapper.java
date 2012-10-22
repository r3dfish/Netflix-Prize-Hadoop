///////////////////////////////////////////////////////////////////////////////
//
//  Copyright (c) 2009, Arizona State University
//  All rights reserved.
//  BSD License: http://www.opensource.org/licenses/bsd-license.html
//  Author: Jeff Conner
//
///////////////////////////////////////////////////////////////////////////////

package ImageProcessingMR;

import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

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

public class IPMRMapper extends MapReduceBase implements Mapper<Text, BytesWritable, Text, BytesWritable> 
{
	// /////////////////////////////////////////////////////////////////////////////
	//
	// Map
	//
	// /////////////////////////////////////////////////////////////////////////////

	public void map(Text key, BytesWritable value,
			OutputCollector<Text, BytesWritable> output, Reporter reporter)
			throws IOException 
	{
		// debug
		System.out.println( "map called in IPMRMapper" );
		
		// create an input stream to parse the value data
		ByteArrayInputStream bis = new ByteArrayInputStream( value.getBytes(), 0, value.getLength() );		
				
		// read the image from the input stream
		BufferedImage image = ImageIO.read( bis );
		
		// Create an edge detector object
		CannyEdgeDetector detector = new CannyEdgeDetector();
		
		// load the original image into the edge detector
		detector.setSourceImage( image );

		// process the image
		detector.process();
		
		// create the output file name
		String filename = new String( key.toString() );
		filename = UsulFilePath.cleanString( filename );
		filename = UsulFilePath.directory( filename ) + UsulFilePath.base( filename ) + "_edmr.jpg";
		
		// Create a configuration object
		Configuration config = new Configuration();

		// Create a filesystem object
		FileSystem hdfs = FileSystem.get(config);
		
		// create a path object from the filename
		Path path = new Path(filename);

		// set the bytes to write
		ByteArrayOutputStream bos = new ByteArrayOutputStream(1000);
		ImageIO.write(detector.getEdgesImage(), "jpg", bos);
		byte[] byteImage = bos.toByteArray();
		
		// Create an output stream
		FSDataOutputStream os = hdfs.create(path);
		
		// write the file
		os.write(byteImage, 0, byteImage.length);

		// close the stream
		os.close();
		
	}
}
