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


public class OverlappingImageRecordReader  implements RecordReader<Text,BytesWritable>
{	
	///////////////////////////////////////////////////////////////////////////////
	//
	// Member Variables
	//
	///////////////////////////////////////////////////////////////////////////////

	private boolean 		_done;
	private String			_filename;
	private BufferedImage   _image;
	private int				_currentX;
	private int 			_currentY;
	private int				_wStepSize;
	private int 			_hStepSize;	
	private int				_buffer;
	
	@SuppressWarnings("unused")
	private JobConf			_conf;	
	
	///////////////////////////////////////////////////////////////////////////////
	//
	// Constructor
	//
	///////////////////////////////////////////////////////////////////////////////

	public OverlappingImageRecordReader(JobConf conf, FileSplit split) throws IOException
	{
		// Store the job conf
		_conf = conf;
		
		// Get the filename
		_filename = split.getPath().toString();
				
		// Are we done?
		_done = false;
		
		// set the image
		Configuration config = new Configuration();
		FileSystem hdfs = FileSystem.get(config);
		Path path = new Path( _filename );
		FSDataInputStream imageStream = hdfs.open(path);
		_image = ImageIO.read(imageStream);
		
		// analyze the image
		_analyzeImage();
		
		// directory
		String directory = UsulFilePath.directory( _filename );
		
		// basename
		String basename = UsulFilePath.base( _filename );
		
		// create the directory to hold the sub files
		Path dirPath = new Path( directory + basename + "_output" );
		hdfs.mkdirs( dirPath );
		
		// close the stream
		//hdfs.close();
		
		// set the image buffer zone
		_buffer = 3;
		
	}


	
	///////////////////////////////////////////////////////////////////////////////
	//
	// Examine the dimensions and set splitting size
	//
	///////////////////////////////////////////////////////////////////////////////

	private void _analyzeImage()
	{
		// set the current width and height
		_currentX = 0;
		_currentY = 0;
		
		// get the dimensions of the image
		int width = _image.getWidth();
		int height = _image.getHeight();
		
		//------------------------------------------------
		// set the splitting to no more than 100x100
		//------------------------------------------------
		
		// set the max width and height
		int maxWidth = 100;
		int maxHeight = 100;
		
		boolean done = false;
		while( false == done )
		{
			// split the width if it is > than the max width
			if( width > maxWidth )
			{
				width /= 2;
			}
			
			// split the height if it is > than the max height
			if( height > maxHeight )
			{
				height /= 2;
			}
			
			// if width and height are at acceptable levels, quit
			if( width < maxWidth && height < maxHeight )
			{
				done = true;
			}
		}
		
		_wStepSize = width;
		_hStepSize = height;
		
	}
	
	
	///////////////////////////////////////////////////////////////////////////////
	//
	// close
	//
	///////////////////////////////////////////////////////////////////////////////

	public void close() throws IOException 
	{
		// Debug
		//System.out.println( "close() called in the ImageRecordReader" );
		
	}

	
	///////////////////////////////////////////////////////////////////////////////
	//
	// Create an empty key
	//
	///////////////////////////////////////////////////////////////////////////////

	public Text createKey() 
	{
		// Debug
		//System.out.println( "createKey() called in the ImageRecordReader" );
		
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
		//System.out.println( "createValue() called in the ImageRecordReader" );
		
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
		//System.out.println( "getPos() called in the ImageRecordReader" );
		
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
		//System.out.println( "getProgress() called in the ImageRecordReader" );
		
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
		// System.out.println( "The value of _done in the Recorder is: " + _done );
		
		if( false == _done )
		{
			
			BufferedImage img = _getNextImage();
			
			// get the image width and height
			int startX = _buffer;
			int startY = _buffer;
						
			// create the key for this sub image
			String key = new String( _filename + " " + _currentX + " " + _currentY + " " + startX + " " + startY + " " + _wStepSize + " " + _hStepSize );
			
			// set the filename ( key )
			filename.set( key );
			
			// get the next image
			ByteArrayOutputStream bos = new ByteArrayOutputStream(1000);
			ImageIO.write(img, "jpg", bos);
			byte[] byteImage = bos.toByteArray();
			
			// set the value
			image.set( byteImage, 0, byteImage.length );		
			
			// update the position
			_updatePosition();
			
			return true;
		}
		else
		{			
			return false;
		}
		
	}

	
	///////////////////////////////////////////////////////////////////////////////
	//
	// Get the next image
	//
	///////////////////////////////////////////////////////////////////////////////

	private BufferedImage _getNextImage()
	{
		// tracing
		//System.out.println( "Method _getNextImage() called for file: " + _filename + " with cX=" + _currentX + " | cY=" + _currentY + " | w=" + _wStepSize + " | h=" + _hStepSize );
		
		// get the height and width step size for this step
		int width = _wStepSize + _buffer;
		int height = _hStepSize + _buffer;
		
		// make sure the width doesn't extend past the image bounds
		if( ( _currentX + width ) > _image.getWidth() )
		{
			width = _image.getWidth() - _currentX;
		}
		
		// make sure the height doesn't extend past the image bounds
		if( ( _currentY + height ) > _image.getHeight() )
		{
			height = _image.getHeight() - _currentY;
		}
		
		// set the start X
		int startX = _currentX;
		int startY = _currentY;
		
		// adjust the x
		if( startX > _buffer )
			startX -= _buffer;
		
		// adjust the Y
		if( startY > _buffer )
			startY -= _buffer;
			
		// get the sub image to send
		BufferedImage image = _image.getSubimage( startX, startY, width, height );
		
		return image;
	}
	
	
	///////////////////////////////////////////////////////////////////////////////
	//
	// Get the next image
	//
	///////////////////////////////////////////////////////////////////////////////

	private void _updatePosition()
	{
		// tracing
		//System.out.println( "Method _updatePosition() called for file: " + _filename + " with cX=" + _currentX + " | cY=" + _currentY + " | w=" + _wStepSize + " | h=" + _hStepSize );

		// get the height and width step size for this step
		int width = _wStepSize - _buffer;
		int height = _hStepSize - _buffer;
		
		// check to see if we are at the end of the row
		if( ( _currentX + width ) >= _image.getWidth() )
		{
			_currentX = 0;
			
			// make sure the height doesn't extend past the image bounds
			if( ( _currentY + height ) <= _image.getHeight() )
			{
				_currentY += height;
			}
			else
			{
				// We are done
				_done = true;
			}
			
		}
		// not at the end of the row so go ahead and increment x and leave y alone
		else
		{
			_currentX += width;
		}		
		
	}

}
