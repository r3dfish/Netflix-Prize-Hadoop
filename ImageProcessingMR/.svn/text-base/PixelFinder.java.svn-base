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
import java.awt.image.Raster;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import javax.imageio.ImageIO;

public class PixelFinder 
{
	///////////////////////////////////////////////////////////////////////////////
	//
	// Member Variables
	//
	///////////////////////////////////////////////////////////////////////////////

	private BufferedImage _sourceImage;
	private List<String> _locations;
	
	///////////////////////////////////////////////////////////////////////////////
	//
	// Constructor
	//
	///////////////////////////////////////////////////////////////////////////////

	public PixelFinder( BufferedImage image )
	{
		_sourceImage = image;
		_locations = new ArrayList<String>();
	}
	
	
	///////////////////////////////////////////////////////////////////////////////
	//
	// Search the image for the given pixel and return its location.
	// r,g,b = red, green blue color of the pixel to find
	// 
	//
	///////////////////////////////////////////////////////////////////////////////

	public void find( int[] searchPixel )
	{
		// Raster of the input data
		Raster raster = _sourceImage.getData();
						
		// get the width
		int width = _sourceImage.getWidth();
		
		// get the height
		int height = _sourceImage.getHeight();
		
		// loop through the image
		for( int i = 0; i < width; ++i )
		{
			for( int j = 0; j < height; ++j )
			{
				// pixel at location i, j
				int[] seed = new int[3];
				int[] pixels = raster.getPixel( i, j, seed );
				
				// Does the search pixel match the current pixel
				if( pixels[0] == searchPixel[0] && pixels[1] == searchPixel[1] && pixels[2] == searchPixel[2] )
				{
					_locations.add( i + " " + j );
				}
			}
		}
		
	}
	
	///////////////////////////////////////////////////////////////////////////////
	//
	// Return a list of location in the image where the input pixel was found.
	//
	///////////////////////////////////////////////////////////////////////////////

	public List<String> getLocations()
	{
		return _locations;
	}
	
	
	///////////////////////////////////////////////////////////////////////////////
	//
	// Testing
	//
	///////////////////////////////////////////////////////////////////////////////
	
	public static void main( String args[] ) throws IOException
	{
		String filename = "C:/data/pixel_finder/test01.jpg";
		
		if( args.length > 0 )
		{
			filename = args[0];
		}
		
		File file = new File( filename );
		BufferedImage image = ImageIO.read( file );
		
		PixelFinder finder = new PixelFinder( image );
		
		int[] searchPixel = {255,255,255};
		finder.find( searchPixel );
		
		ArrayList<String> results = (ArrayList<String>)finder.getLocations();
		
		System.out.println( "\n\n\n****************Results****************" );
		
		for( int i = 0; i < results.size(); ++i )
		{
			System.out.println( "Search Pixel Found At: " + (String)results.get( i ) );
		}
		
	}
	

}
