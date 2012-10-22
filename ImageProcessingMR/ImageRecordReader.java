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
import java.io.ByteArrayOutputStream;
import java.io.IOException;

import javax.imageio.ImageIO;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;

public class ImageRecordReader  implements RecordReader<Text,BytesWritable>
{	
	///////////////////////////////////////////////////////////////////////////////
	//
	// Member Variables
	//
	///////////////////////////////////////////////////////////////////////////////

	private boolean 		_done;
	private String			_filename;
	
	///////////////////////////////////////////////////////////////////////////////
	//
	// Constructor
	//
	///////////////////////////////////////////////////////////////////////////////

	public ImageRecordReader(JobConf conf, FileSplit split) throws IOException
	{
		// Debug
		System.out.println( "Constructor called in the ImageRecordReader" );
		
		// Get the filename
		_filename = split.getPath().toString();
				
		// Are we done?
		_done = false;
		
	}


	///////////////////////////////////////////////////////////////////////////////
	//
	// close
	//
	///////////////////////////////////////////////////////////////////////////////

	public void close() throws IOException 
	{
		// Debug
		System.out.println( "close() called in the ImageRecordReader" );
		
	}

	
	///////////////////////////////////////////////////////////////////////////////
	//
	// Create an empty key
	//
	///////////////////////////////////////////////////////////////////////////////

	public Text createKey() 
	{
		// Debug
		System.out.println( "createKey() called in the ImageRecordReader" );
		
		return new Text();
	}

	
	///////////////////////////////////////////////////////////////////////////////
	//
	// Create an empty value
	//
	///////////////////////////////////////////////////////////////////////////////

	public BytesWritable createValue() 
	{
		// Debug
		System.out.println( "createValue() called in the ImageRecordReader" );
		
		return new BytesWritable();
	}

	
	///////////////////////////////////////////////////////////////////////////////
	//
	// Get the current file position
	//
	///////////////////////////////////////////////////////////////////////////////

	public long getPos() throws IOException 
	{
		// Debug
		System.out.println( "getPos() called in the ImageRecordReader" );
		
		return 0;
	}

	
	///////////////////////////////////////////////////////////////////////////////
	//
	// Progress
	//
	///////////////////////////////////////////////////////////////////////////////

	public float getProgress() throws IOException 
	{
		// Debug
		System.out.println( "getProgress() called in the ImageRecordReader" );
		
		return 0;
	}

	
	///////////////////////////////////////////////////////////////////////////////
	//
	// Get the next key/value pair
	//
	///////////////////////////////////////////////////////////////////////////////

	public boolean next(Text filename, BytesWritable image ) throws IOException 
	{
		// Debug
		System.out.println( "next called in the ImageRecordReader" );
		
		if( false == _done )
		{
			// set the filename ( key )
			filename.set( _filename );
			
			// Debug
			// System.out.println( "The filename in the recorder is: " + _filename );
			
			// 
			Configuration config = new Configuration();
			FileSystem hdfs = FileSystem.get(config);
			Path path = new Path( _filename );
			FSDataInputStream imageStream = hdfs.open(path);
			BufferedImage bufferedImage = ImageIO.read(imageStream);
			ByteArrayOutputStream bos = new ByteArrayOutputStream(1000);
			ImageIO.write(bufferedImage, "jpg", bos);
			byte[] byteImage = bos.toByteArray();
			
			image.set( byteImage, 0, byteImage.length );
			
			// debug
			// System.out.println( "This size of the image in the Recorder is: " + image.getLength() );
			
			_done = true;
			
			return true;
		}
		else
		{			
			return false;
		}
		
	}

}
