import threading
import time 
import os

#
# Grid
#
# If you are on the Grid, you can never leave.
#

class Grid(object):
	def __init__(self, scheduler_func):
		# For jobs that have partial file uploads or that 
		# aren't quite ready to make it into the queue yet
		self.tmp_jobs = []

		self.jobs = {}
		self.job_ids = {}
		self.next_job_id = 0
		
		self.nodes = {}
		self.node_ids = {}
		self.next_node_id = 0

		self.scheduler = scheduler_func(self)
		self.scheduler.start()

	def tmp_job_create(self, executable, name):
		tmpid = len(self.tmp_jobs)
		self.tmp_jobs.append( Job(executable, name) )

		# Make sure that we have an empty folder to put uploads in
		path = "../data/" + str(tmpid)

		# Make sure that we have a root folder where it needs to be..
		# a bit of a hack, but atleast it will work first time 
		try:
			os.mkdir("../data/")
		except(OSError) as e:
			pass

		try:
			os.mkdir(path)
		except(OSError) as e:
			pass

		# Remove anything in there...
		for the_file in os.listdir(path):
		    file_path = os.path.join(path, the_file)
		    try:
		        if os.path.isfile(file_path):
		            os.unlink(file_path)
		    except Exception, e:
		        print e

		return tmpid

	def tmp_job_update(self, tmpid, executable, name):
		self.tmp_jobs[tmpid].executable = executable
		self.tmp_jobs[tmpid].name = name


		return tmpid


	def tmp_job_add_file(self, tmpid, filename, raw):

		path = "../data/" + str(tmpid) + "/" + filename
		fp = open(path, "w+")
		fp.write(raw)
		fp.close()

		self.tmp_jobs[tmpid].add_file(filename)

	# Sets a temp job as being ready, and places it in the
	# queue ready for processing
	def tmp_job_enqueue(self, tmpid):
		job = self.tmp_jobs[tmpid]
		del self.tmp_jobs[tmpid]

		job.enqueue(self.next_job_id)
		self.scheduler.queue.append(job)

		self.next_job_id += 1

		return job.id

	#
	# add_job(self, job)
	#
		
	def add_job(self, job_id):
		pass

	#
	# get_job(self, job_id)
	#

	def get_job(self, job_id):
		pass

	#
	# get_node_id(self, node_ident)
	#
	# Takes a node identifier in form of HOST:PORT and
	# returns the unique identifier of that node
	#

	def get_node_id(self, node_ident):
		if node_ident in self.node_ids:
			return self.node_ids[ node_ident ]
		else:
			raise NodeNotFoundException("There is no node with ident: %s" % node_ident)

	#
	# get_node(self, node_id)
	#
	# Takes a node_is either as an internal id, or as 
	# the string HOST:PORT and returns the node as a dict
	#

	def get_node(self, node_id):

		if isinstance(node_id, str):
			if node_id.isdigit():
				node_id = int(node_id)
			else:
				node_id = self.get_node_id(node_id)

		if node_id in self.nodes:
			return self.nodes[ node_id ]
		else:
			raise NodeNotFoundException("There is no node with id: %s" % node_id)

	#
	# add_node(self, node)
	#
	# Takes a dict containing at minimum a host and port,
	# calculates a unique ID for the host/port if it hasn't
	# seen it before, and returns that ID
	#

	def add_node(self, node):
		node_ident = "%s:%s" % (node['host'], node['port'])

		if node_ident not in self.node_ids:
			self.node_ids[ node_ident ] = self.next_node_id
			self.next_node_id += 1

		node_id = self.get_node_id(node_ident)

		node['node_id'] = node_id
		self.nodes[ node_id ] = node

		return node_id

	# 
	# update_node(self, node_id, update)
	#
	# Takes a node_id and a dict with an update
	# and updates the node dict with the given
	#

	def update_node(self, node_id, update):
		self.get_node(node_id).update(update)
		return self.get_node(node_id)

#
# GridService
#
# THIS SHIT IS DEPRECATED, DON'T USE IT 
# 

class GridService:
	
	def __init__(self):
		self.host = ""
		self.port = ""

	@property
	def url(self):
		return self._url()

	def _url(self):
		return "http://%s:%s" % (self.host, int(self.port))

	def __str__(self):
		return self.url

#
# Scheduler
#
# A generic Scheduler object
#

class Scheduler(object):
	
	JOB_ALLOCATOR_INTERVAL = 2

	def __init__(self, grid):
		self.grid = grid

		self.queue_lock = threading.Lock()
		self.queue = []

	def start(self):
		self.start_job_allocator()

	def start_job_allocator(self):
		self.thread = threading.Thread(target = self.job_allocator)
		self.thread.name = "Master:Grid:Scheduler:JobAllocator"
		self.thread.daemon = True
		self.thread.start()

	def job_allocator(self):
		# For now, as I want to see stuff in the queue...
		return

		while True:
			self.allocate_jobs()
			time.sleep(self.JOB_ALLOCATOR_INTERVAL)

	def allocate_jobs(self):
		raise NotImplementedError()

	def add_to_queue(self, job):
		self.queue.append(job)
		# raise NotImplementedError()

#
# BullshitScheduler
#
# A BullshitScheduling Algorithm
#

class BullshitScheduler(Scheduler):

	def add_to_queue(self, job):
		# Need to ensure thread safety by checking the 
		# queue is not in use before modifying it
		with self.queue_lock:
			self.queue.append(job)

	def allocate_jobs(self):
		with self.queue_lock:
			print self.grid.nodes
			print self.queue
#
# Job
#
# A Job is an executable and any files it needs to be
# run against. A job contains one or more WorkUnits.
#

class Job():
	def __init__(self, executable, name):
		self.executable = executable
		self.files = []
		self.name = name
		self.status = "Awaiting files"
		self.id=-1

	def add_file(self, file):
		self.files.append(file)
		self.status = "Received " + str(len(self.files)) + " files"

	def enqueue(self, id):
		self.status = "Ready"
		self.id = id
		self.create_work_units()

	# def get_files(self):
	# 	return self.files

	# def get_name(self):
	# 	return self.name

	# def get_executable(self):
	# 	return self.executable

	# def count_files(self):
	# 	return length(self.files)

	def count_work_units(self):
		return length(self.work_units)

	def create_work_units(self):
		self.work_units = []
		
		if self.files:
			for filename in self.files:
				self.work_units.append( WorkUnit(self.executable, filename, self.name) )
		else:
			self.work_units.append( WorkUnit(self.executable) )

#
# WorkUnit
#
# A WorkUnit is what is sent to each Node for processing 
# each WorkUnit is to be scheduled independently.
#

class WorkUnit():
	
	def __init__(self, executable, filename, name):
		self.name = name
		self.executable = executable
		self.filename = filename
		# self.created = time.localtime()
		# self.created_asc = time.asctime(self.created)
		self.status = "Queued"
		self.node = "None"

	def get_filename(self):
		return self.filename

	def get_executable(self):
		return self.executable

#
# NodeNotFoundException
#
# To be raised when a node is requested that can't be found
#

class NodeNotFoundException(Exception):
	pass
