#!/usr/bin/python
#################################
# pyMapper.py                   #
# pyNetflix1                    #
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
# Output - MovieID	Rating,Date (<Key,Value> seperated by a tab or \t)
# 

import sys

# Iterator function that returns 1 line at a time
# and strips the whitespace
def read_input(file):
	for line in file:
		yield line.rstrip()

def main():
	
	# Read input through iterator and
	# use a for loop to operate on each line
	input = read_input(sys.stdin)
	for line in input:
		
		# Split the line by comma
		lineSplit = line.split(',')
		
		# Parse only if there are 4 values in a line
		if len(lineSplit) == 4:
			
			# Key = MovieID
			movie = lineSplit[0]
			
			# Value = Rating,Date
			rating = lineSplit[2]
			date = lineSplit[3].replace('-','')
			value = rating+','+date
			
			# Output Key-Value pair
			print '%s\t%s' % (movie, value)
			
			# Output status to Hadoop Reporter
			# (Necessary for Hadoop Streaming Apps)
			print >> sys.stderr, "report:counter:pyNetflix1,mapper,1"


if __name__ == "__main__":
	main()