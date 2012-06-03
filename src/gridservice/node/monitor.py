import subprocess

class Monitor:
	
	#
	# psColumn(self, colName)
	#
	# gets a column from stdout of ps -e -o, and returns it
	# as a list.
	#

	def psColumn(self, colName):
		"""Get a column of ps output as a list"""
		ps = subprocess.Popen(["ps", "-e", "-o", colName], stdout=subprocess.PIPE)
		(stdout, stderr) = ps.communicate()
		column = stdout.split("\n")[1:]
		column = [token.strip() for token in column if token != '']
		return column

	#
	# cpu(self)
	#
	# gets the total cpu usage of the computer
	#

	def cpu(self):
		values = map(float, self.psColumn("%cpu"))
		return sum(values)
