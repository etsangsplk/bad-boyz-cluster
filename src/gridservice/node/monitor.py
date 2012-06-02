import subprocess

class Monitor:
	def psColumn(self, colName):
		"""Get a column of ps output as a list"""
		ps = subprocess.Popen(["ps", "-e", "-o", colName], stdout=subprocess.PIPE)
		(stdout, stderr) = ps.communicate()
		column = stdout.split("\n")[1:]
		column = [token.strip() for token in column if token != '']
		return column

	def cpu(self):
		values = map(float, self.psColumn("%cpu"))
		return sum(values)
