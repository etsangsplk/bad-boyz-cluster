from __future__ import division

#
# Wall Time Parsing functions
#

#
# WallTime(object)
#
# Class for holding a WallTime data structure
#
class WallTime(object):

	#
	# __init__(self, wall_split)
	#
	# Initialises from a list of integers:
	# [DD, HH, MM, SS].
	#
	def __init__(self, wall_split):
		if len(wall_split) != 4:
			raise WallTimeFormatException
		# Check that each unit can actually be made an integer
		try:
			wall_split = map(int, wall_split)
		except (TypeError, ValueError):
			raise WallTimeFormatException
		
		# Handle cases where seconds overflows 60
		self.tm_second = wall_split[3] % 60
		self.tm_minute = (wall_split[3] - self.tm_second) / 60
		
		# Handle cases where minutes overflows 60
		self.tm_minute += wall_split[2] % 60
		self.tm_hour = (wall_split[2] - self.tm_minute) / 60
		
		# Handle cases where hours overflows 24
		self.tm_hour += wall_split[1] % 24
		self.tm_day = (wall_split[1] - self.tm_hour) / 24

		self.tm_day += wall_split[0]


	#
	# tm_day(self) 
	#
	# the days unit of the WallTime object
	# 

	@property
	def tm_day(self):
		return self._tm_day
	
	@tm_day.setter
	def tm_day(self, days):
		self._tm_day = int(days)
	
	#
	# tm_hour(self) 
	#
	# the hours unit of the WallTime object
	# 
	
	@property
	def tm_hour(self):
		return self._tm_hour
	
	@tm_hour.setter
	def tm_hour(self, hours):
		self._tm_hour = int(hours)

	#
	# tm_minute(self) 
	#
	# the minutes unit of the WallTime object
	# 

	@property
	def tm_minute(self):
		return self._tm_minute
	
	@tm_minute.setter
	def tm_minute(self, minutes):
		self._tm_minute = int(minutes)

	#
	# tm_second(self) 
	#
	# the days unit of the WallTime object
	#

	@property
	def tm_second(self):
		return self._tm_second
	
	@tm_second.setter
	def tm_second(self, seconds):
		self._tm_second = int(seconds)
	
	def __str__(self):
		return "[days : %s, hours : %s, minutes : %s, seconds : %s]" % (self.tm_day, self.tm_hour, self.tm_minute, self.tm_second)
	
	def __repr__(self):
		return "[days : %s, hours : %s, minutes : %s, seconds : %s]" % (self.tm_day, self.tm_hour, self.tm_minute, self.tm_second)

#
# strptime(wall_time)
#
# Converts a string of format "D:HH:MM:SS" or "HH:MM:SS" into a
# WallTime datatype
#

def strptime(wall_time):
	# Check that wall_time format is valid
	wall_split = wall_time.split(":")
	# Acceptable to drop DD from the Wall_Time
	if len(wall_split) not in [3,4]:
		raise WallTimeFormatException
	# if DD has been dropped, add it.
	if len(wall_split) == 3:
		wall_split = [00] + wall_split

	wall_strp = WallTime(wall_split)
	return wall_strp

#
# strftime(wall_strp, format):
#
# Formats a wall_strp data structure. Currently only supports
# The format "%D:%H:%M:%S"
#

def strftime(wall_strp, format="%D:%H:%M:%S"):
	if not isinstance(wall_strp, WallTime):
		raise TypeError("Expected object of type WallTime")
	wall_time = "{}:{:02}:{:02}:{:02}".format(wall_strp.tm_day, wall_strp.tm_hour, wall_strp.tm_minute, wall_strp.tm_second)
	return wall_time
	
#
# wall_days(wall_strp)
#
# returns the number of days as a fraction of a wall_strp datatype
#

def wall_days(wall_strp):
	if not isinstance(wall_strp, WallTime):
		raise TypeError("Expected object of type WallTime")
	return (wall_strp.tm_day) + (wall_strp.tm_hour / 24) + (wall_strp.tm_minute / 1440) + (wall_strp.tm_second / 86400) 

#
# wall_hours(wall_strp)
#
# returns the number of hours as a fraction of a wall_strp datatype
#

def wall_hours(wall_strp):
	if not isinstance(wall_strp, WallTime):
		raise TypeError("Expected object of type WallTime")
	return (wall_strp.tm_day * 24) + (wall_strp.tm_hour) + (wall_strp.tm_minute / 60) + (wall_strp.tm_second / 3600)

#
# wall_mins(wall_strp)
#
# returns the number of minutes as a fraction of a wall_strp datatype
#

def wall_mins(wall_strp):
	if not isinstance(wall_strp, WallTime):
		raise TypeError("Expected object of type WallTime")
	return (wall_strp.tm_day * 1440) + (wall_strp.tm_hour * 60) + (wall_strp.tm_minute) + (wall_strp.tm_second / 60)

#
# wall_secs(wall_strp)
#
# returns the number of seconds of a wall_strp datatype
#

def wall_secs(wall_strp):
	if not isinstance(wall_strp, WallTime):
		raise TypeError("Expected object of type WallTime")
	return (wall_strp.tm_day * 86400) + (wall_strp.tm_hour * 3600) + (wall_strp.tm_minute * 60) + (wall_strp.tm_second)
	
#
# WallTimeFormatException(Exception) 
#

class WallTimeFormatException(Exception):
	pass
