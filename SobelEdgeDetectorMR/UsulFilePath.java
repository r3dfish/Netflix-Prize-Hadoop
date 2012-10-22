///////////////////////////////////////////////////////////////////////////////
//
//  Copyright (c) 2009, Arizona State University
//  All rights reserved.
//  BSD License: http://www.opensource.org/licenses/bsd-license.html
//  Author: Jeff Conner
//
///////////////////////////////////////////////////////////////////////////////

package SobelEdgeDetectorMR;

public class UsulFilePath 
{
	///////////////////////////////////////////////////////////////////////////////
	//
	//  Returns the directory (if present) if the input string
	//
	///////////////////////////////////////////////////////////////////////////////

	public static String directory( String path )
	{
		// value to hold the return string
		String dir = new String();
			
		// value to hold the index
		int index = -1;
		
		// iterate through the string to find the index of the last slash
		for( int i = 0; i < path.length(); ++i )
		{
			// if we found a slash...
			if( '/' == path.charAt( i ) )
			{
				// update the index location
				index = i;
			}	
		}
		
		// if there was a slash then set the directory
		if( index >= 0 )
		{
			// set the return string to the directory string
			dir = path.substring( 0, index + 1 );
		}
		
		
		return dir;
	}
	
	
	///////////////////////////////////////////////////////////////////////////////
	//
	//  Returns the extension (if present) if the path
	//
	///////////////////////////////////////////////////////////////////////////////
	
	public static String extension( String path )
	{
		// variable to hold the extension
		String ext = new String();

		// Value to hold the index of the beginning of the extension
		int index = -1;
		
		// loop through the string from back to front to find the extension
		for( int i = path.length() - 1; i >= 0; --i )
		{
			if( '.' == path.charAt( i ) )
			{
				// set the index
				index = i;
				
				// end the loop
				break;
			}
		}
		
		// if there was a . found
		if( index > 0 )
		{
			// set the extension
			ext = path.substring( index, path.length() );
		}
		
		return ext;
	}
	
	
	///////////////////////////////////////////////////////////////////////////////
	//
	//  Returns the base filename from the input path
	//
	///////////////////////////////////////////////////////////////////////////////

	public static String base ( String path )
	{		
		// variable to hold the filename result
		String filename = new String( path );	

		// get the directory
		String directory = UsulFilePath.directory( path );
		
		// get the extension
		String extension = UsulFilePath.extension( path );
		
		// if there is an extension strip it off
		if( false == extension.isEmpty() )
		{	
			// Strip the extension
			filename = filename.substring( 0, filename.length() - extension.length() );
		}
		
		// if there is a drectory...
		if( directory.length() > 0 )
		{
			// strip the directory
			filename = filename.substring( directory.length(), filename.length() );
		}
		
		return filename;
	}
}
