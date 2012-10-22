# netflixReorg.awk written to reorganize the netflix prize data set
# for use with a hadoop cloud of 100 nodes
# Written by Joey Calca
# r3dfish@hackedexistence.com
# http://hackedexistence.com

# Tokenize lines on ":"
# This is used to get rid of the ":" at the end of
# the first line of each file
BEGIN { FS = ":" }

# If it is the first line of the file
# The first token is the movieID
{if( FNR == 1) movieID = $1

# If it is not the first line of the file
# output movieID "," first token
if ( FNR != 1 ) print movieID "," $1}
