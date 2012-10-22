#!/usr/bin/python
#################################
# pyMapper.py                   #
# pyNetflix2                    #
#                               #
# by Ryan Anguiano              #
# bl4ckbird@hackedexistence.com #
#################################

# 
# pyMapper takes its input from the reorganized NetFlix Prize Dataset.
# It iterates through eath file in the training_set_reorg folder.
# Each line is passed to this program through sys.stdin and the
# data is operated on, then passed to sys.stdout. Sample lines:
# 
# Input - MovieID,UserID,Rating,Date
# Output - UserID	MovieID,Rating,RatingDelay
# (<Key,Value> seperated by a tab or \t)
# 

import sys

# Iterator function that returns 1 line at a time
# and strips the whitespace
def read_input(file):
	for line in file:
		yield line.rstrip()

# Import the movie titles file from distributed cache,
# and load it into a python dict so each movie year can
# be accessed by a MovieID key
def dict_from_movie_cache(file_name):
	
	# Create a new dictionary and open the movie titles file
	newDict = {}
	f = open(file_name)
	
	# For every line in the file split the line by 2 commas
	# and distribute the values to movieID, year, name.
	# Assign each movie to the dictionary with the key being
	# movieID and the value being the movie year.
	for line in f:
		mov, year, name = line.rstrip().split(',',2)
		newDict[mov] = year
	
	# Close the file and return the dictionary
	f.close()
	return newDict

def main():
	
	# Read input through iterator and load movie titles
	input = read_input(sys.stdin)
	movie_years = dict_from_movie_cache('movie_titles.txt')
	
	# For loop to operate on each line
	for line in input:
		
		# Split the line by comma
		lineSplit = line.split(',')
		
		# Parse only if there are 4 values in a line
		if len(lineSplit) == 4:
			
			# Key = UserID
			user = lineSplit[1]
			
			# Value = MovieID,Rating,RatingDelay
			movie = lineSplit[0]
			rating = lineSplit[2]
			
			ratingDate = lineSplit[3]
			
			# There are some 'NULL' year fields in the dataset,
			# so a try-except is used here to catch 'NULL' and
			# ignore the movie
			try:
				
				# Rating year is the first 4 digits of the date
				# Movie year is read from the movie_years dict
				ratingYear = ratingDate[:4]
				movieYear = movie_years[movie]
				
				# The rating delay is: rating year - movie year
				ratingDelay = int(ratingYear) - int(movieYear)
				
				value = movie+','+rating+','+str(ratingDelay)
				
				# Output Key-Value pair
				print '%s\t%s' % (user, value)
				
				# Output status to Hadoop Reporter
				# (Necessary for Hadoop Streaming Apps)
				print >> sys.stderr, "report:counter:pyNetflix2,mapper,1"
			
			# If the year value is 'NULL' then report the movieID to
			# the tracker.
			except:
				print >> sys.stderr, "report:status:InputFormat Error. MovieID: %s" % movie


if __name__ == "__main__":
	main()