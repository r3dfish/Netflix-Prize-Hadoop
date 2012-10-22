#!/usr/bin/python
#################################
# pyReducer.py                  #
# pyNetflix2                    #
#                               #
# by Ryan Anguiano              #
# bl4ckbird@hackedexistence.com #
#################################

# 
# pyReducer takes its input from pyMapper and groups the
# values by key. It then iterates through each key and
# accumulates and finds the average the users ratings and
# rating delays. It operates on each movie key and outputs
# one line based on each key in the following format:
# 
# UserID numRatings,avgRating,avgDelay
# (<Key,Value> seperated by a tab or \t)
# 

from itertools import groupby
from operator import itemgetter
import sys

# Iterator function that returns and separates
# each line by a given separator
def read_mapper_output(file, separator='\t'):
	for line in file:
		yield line.rstrip().split(separator, 1)

def main():
	
	# Read input through iterator and load movie titles
	mapper = read_mapper_output(sys.stdin)
	
	# For loop to iterate through each line
	# (Please note that for loop is necessary although
	# not used because of the groupby function used below)
	for line in mapper:
		
		# Groupby is an python iterator tool that is
		# grouping all mapper input by MovieID
		for user, group in groupby(mapper, itemgetter(0)):
			try:
				
				# numRatings is the total number of ratings per user
				# totalRatings is the accumulated total of the ratings
				# totalDelay is the accumulated total of ratingDelays
				numRatings = 0
				totalRating = 0
				totalDelay = 0
				
				# Iterate through each movie rated by a user
				for user, value in group:
					
					# Accumulate ratings and count
					movie, rating, ratingDelay = value.split(',',2)
					numRatings += 1
					totalRating += int(rating)
					totalDelay += int(ratingDelay)
				
				# Average together all the ratings by a user
				avgRating = float(totalRating) / numRatings
				avgDelay = float(totalDelay) / numRatings
				
				# Concatenate all the values
				value = '%d,%f,%f' % (numRatings, avgRating, avgDelay)
				# Output Key-Value pair
				print '%s\t%s' % (user, value)
				
				# Output status to Hadoop Reporter
				print >> sys.stderr, "report:counter:pyNetflix2,reducer,1"
			
			except ValueError:
				print >> sys.stderr, "report:status:Reducer Error UserID: %s" % user

if __name__ == "__main__":
	main()