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
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import javax.imageio.ImageIO;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;


public class SEDMRReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text> 
{
	// /////////////////////////////////////////////////////////////////////////////
	//
	// Reducer
	//
	// /////////////////////////////////////////////////////////////////////////////

	public void reduce(Text key, Iterator<Text> values,
			OutputCollector<Text, Text> output, Reporter reporter)
			throws IOException 
	{
		// make a copy of the original image
		Configuration config = new Configuration();
		FileSystem hdfs = FileSystem.get(config);
		
		// get the original filename
		String ofn = key.toString();
		
		// set the source path to the original filename
		Path srcPath = new Path( ofn );
		
		// get the source image directory
		String directory = UsulFilePath.directory( ofn );
		
		// get the source base name
		String base = UsulFilePath.base( ofn );
		
		// set the destination path
		Path dstPath = new Path( new String( directory + base + "_sobel.jpg" ) );
		
		// make a BufferedImage from the path
		FSDataInputStream srcStream = hdfs.open( srcPath );
		BufferedImage image = ImageIO.read( srcStream );
		
		// collect and pass on the output
		while( values.hasNext() )
		{			
			// Grab a value
			String value = values.next().toString();
			
			// tokenize the value so we can iterate through the strings
			StringTokenizer itr = new StringTokenizer( value );
	    
		    // get the filename
		    String filename = itr.nextToken();
		    
		    // get the starting x
		    int x = Integer.parseInt( itr.nextToken() );
		    
		    // get the starting y
		    int y = Integer.parseInt( itr.nextToken() );
		    
		    // get the width
		    int sx = Integer.parseInt( itr.nextToken() );
		    
		    // get the height
		    int sy = Integer.parseInt( itr.nextToken() );
		    
		    // get the width
		    int wStep = Integer.parseInt( itr.nextToken() );
		    
		    // get the height
		    int hStep = Integer.parseInt( itr.nextToken() );
		    		    
		    // get the sub image		    
			FSDataInputStream is = hdfs.open( new Path( filename ) );
			BufferedImage subimage = ImageIO.read( is );
			
			// some debug information
			int iw = subimage.getWidth() - 1;
			int ih = subimage.getHeight() - 1;
			
			// get the data from the sub image
			int[] subdata;
			
			if( iw > sx && ih > sy )
			{
				if( x == 0 )
				{
					sx = 0;
				}
				if( y == 0 )
				{
					sy = 0;
				}
				
				subdata = new int[ iw * ih ];
				subimage.getRGB( sx, sy, iw - sx, ih - sy , subdata, 0, iw - sx );
				
				// set the data in the corresponding position in the larger image
				image.setRGB( x, y, iw - sx, ih - sy, subdata, 0, iw - sx);
			}
			else
			{
				subdata = new int[ iw * ih ];
				subimage.getRGB( 0, 0, iw , ih , subdata, 0, iw );
				
				// set the data in the corresponding position in the larger image
				image.setRGB( x, y, iw, ih, subdata, 0, iw);
			}
				
			
			//close the sub image
			is.close();
			
			// collect the output
			output.collect( new Text( key.toString() ), new Text( value ) );
			
		}
				
		// set the bytes to write
		ByteArrayOutputStream bos = new ByteArrayOutputStream(1000);
		ImageIO.write(image, "jpg", bos);
		byte[] byteImage = bos.toByteArray();		
		
		// Create an output stream
		FSDataOutputStream os = hdfs.create( dstPath );
		
		// write the file
		os.write(byteImage, 0, byteImage.length);

		// close the stream
		os.close();
	}
}
