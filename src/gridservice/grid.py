import threading
from time import sleep

#
# Grid
#
# If you are on the Grid, you can never leave.
#

class Grid(object):
	def __init__(self, scheduler):
		self.nodes = {}
		self.start_scheduler(scheduler)

	def add_node(self, node):
		self.nodes[ node['ip_address'] + ":" + node['port'] ] = node

	def start_scheduler(self, scheduler):

		# Setting daemon = True causes the thread to 
		# be closed with the main program
		self.thread = SchedulerThread(scheduler)
		self.thread.daemon = True
		self.thread.start()

	def __str__(self):
		return str(self.nodes)

#
# SchedulerThread
#
# A thread for the scheduler, will run in its own thread
# on Master checking the queue and allocating jobs.
#

class SchedulerThread(threading.Thread):
	def __init__(self, scheduler):
		super(SchedulerThread, self).__init__()
		self.scheduler = scheduler

	def run(self):
		while True:
			self.scheduler.allocate_jobs()

#
# Scheduler
#
# A generic Scheduler object
#

class Scheduler(object):
	def __init__(self):
		self.queue_lock = threading.Lock()
		self.queue = []
		
	def add_to_queue(self, job):
		
		# Need to ensure thread safety by checking the 
		# queue is not in use before modifying it
		with self.queue_lock:
			self.queue.append(job)

	def allocate_jobs(self):
		with self.queue_lock:
			print self.queue
		
		# Do not put the sleep inside the lock
		sleep(2)

#
# RoundRobinScheduler
#
# A Round Robin Scheduling Algorithm
#

class RoundRobinScheduler(Scheduler):
	pass

#
# Job
#
# A Job is an executable and any files it needs to be
# run against. A job contains one or more WorkUnits.
#

class Job(object):
	def __init__(self, executable, files=[]):
		self.executable = executable
		self.files = files
		self.create_work_units()

	def get_files(self):
		return self.files

	def get_executable(self):
		return self.executable

	def count_files(self):
		return length(self.files)

	def count_work_units(self):
		return length(self.work_units)

	def create_work_units(self):
		self.work_units = []
		
		if self.files:
			for filename in self.files:
				self.work_units.append( WorkUnit(self.executable, filename) )
		else:
			self.work_units.append( WorkUnit(self.executable) )

#
# WorkUnit
#
# A WorkUnit is what is sent to each Node for processing 
# each WorkUnit is to be scheduled independently.
#

class WorkUnit(object):
	
	def __init__(self, executable, filename):
		self.executable = executable
		self.filename = filename

	def get_filename(self):
		return self.filename

	def get_executable(self):
		return self.executable
