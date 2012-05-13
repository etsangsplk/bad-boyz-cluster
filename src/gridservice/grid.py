import threading
import time 
import json
import copy
import os
from urllib2 import HTTPError, URLError
from httplib import HTTPException

from gridservice.http import JSONHTTPRequest

#
# Grid
#
# If you are on the Grid, you can never leave.
#

class Grid(object):
	def __init__(self, scheduler_func):
		self.jobs = {}
		self.next_job_id = 0
		
		self.nodes = {}
		self.node_ids = {}
		self.next_node_id = 0

		self.scheduler = scheduler_func(self)
		self.scheduler.start()

	#
	# get_free_node
	#
	# A generator of node that have at least 1 core free
	#

	def get_free_node(self):
		for node in self.nodes.values():
			if (node['cores'] - len(node['work_units']) > 0):
				yield node

	#
	# add_job(self, job_data)
	#
		
	def add_job(self, executable, flags, wall_time, deadline, budget):
		
		job = Job(
			job_id = self.next_job_id,
			executable = executable, 
			flags = flags, 
			wall_time = wall_time, 
			deadline = deadline, 
			budget = budget
		)

		self.jobs[ self.next_job_id ] = job
		self.next_job_id += 1

		return job

	#
	# get_job(self, job_id)
	#

	def get_job(self, job_id):	

		if isinstance(job_id, str) and job_id.isdigit():
			job_id = int(job_id)

		if job_id in self.jobs:
			return self.jobs[ job_id ]
		else:
			raise JobNotFoundException("There is no job with id: %s" % job_id)

	#
	# update_job_status(self, job_id, status)
	#

	def update_job_status(self, job_id, status):
		if status not in [ "READY" ]:
			raise InvalidJobStatusException("The job status %s is not valid." % status)

		job = self.get_job(job_id)
		
		if status == "READY":
			job.ready()
			self.scheduler.add_to_queue(job)

		return job

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
			node.update({'created_ts': int(time.time())})
			self.node_ids[ node_ident ] = self.next_node_id
			self.next_node_id += 1
		
		node.update({'came_online_ts': int(time.time()), 'heartbeat_ts': int(time.time())})

		node_id = self.get_node_id(node_ident)

		node['node_id'] = node_id
		node['work_units'] = []

		self.nodes[ node_id ] = node

		return node_id

	# 
	# update_node(self, node_id, update)
	#
	# Takes a node_id and a dict with an update
	# and updates the node dict with the given
	#

	def update_node(self, node_id, update):
		update.update({'heartbeat_ts': int(time.time())})

		self.get_node(node_id).update(update)
		return self.get_node(node_id)

	#
	# node_to_dict(self, node)
	#
	# For some stupid reason Nodes are still not an object,
	# but are a dict, so we need some fancy converting to
	# output a node nicely.
	#

	def node_to_dict(self, node):
		n = copy.copy(node)

		n['work_units'] = []
		for unit in node['work_units']:
			n['work_units'].append(unit.to_dict())

		return n

#
# Scheduler
#
# A generic Scheduler object
#

class Scheduler(object):
	
	WORK_UNIT_ALLOCATOR_INTERVAL = 2

	def __init__(self, grid):
		self.grid = grid

		self.queue_lock = threading.Lock()
		self.queue = []

	def start(self):
		self.start_work_unit_allocator()

	def get_queued(self):
		return [unit for unit in self.queue if unit.status == "QUEUED"]

	def start_work_unit_allocator(self):
		self.thread = threading.Thread(target = self.work_unit_allocator)
		self.thread.name = "Master:Grid:Scheduler:WorkUnitAllocator"
		self.thread.daemon = True
		self.thread.start()

	def work_unit_allocator(self):
		while True:
			self.allocate_work_units()
			time.sleep(self.WORK_UNIT_ALLOCATOR_INTERVAL)

	def node_url(self, node):
		return "http://%s:%s" % (node['host'], node['port'])

	def allocate_work_unit(self, node, work_unit):

		print "Allocating Work Unit from Job %s to Node %s:%s" % (work_unit.job.job_id, node['host'], node['port'])

		work_unit.running(node['node_id'])
		node['work_units'].append(work_unit)

		try:
			request = JSONHTTPRequest( 'POST', self.node_url(node) + '/task', {
				'job_id': work_unit.job.job_id,
				'executable': work_unit.job.executable,
				'flags': work_unit.job.flags,
				'filename': work_unit.filename,
				'wall_time': work_unit.job.wall_time,
			})
		except (HTTPException, URLError) as e:
			pass

	def add_to_queue(self, job):
		with self.queue_lock:
			for work_unit in job.work_units:
				self.queue.append(work_unit)

	def allocate_work_units(self):
		with self.queue_lock:
			print self.grid.jobs
			for node in self.grid.get_free_node():
				unit = self.next_work_unit()

				if unit == None:
					print "Job Queue is empty."
					break

				self.allocate_work_unit(node, unit)

	#
	# next_work_unit(self)
	# 
	# This is the workhorse of the scheduler, this function will
	# look through the list of queued work units and decide what
	# needs to be allocated next. It returns a dict which is
	# the node that the work unit is going to be allocated to.
	#

	def next_work_unit(self):
		raise NotImplementedError()


#
# BullshitScheduler
#
# A BullshitScheduling Algorithm
#

class BullshitScheduler(Scheduler):

	# Are you ready for the worlds most advanced 
	# scheduling algorithm?

	def next_work_unit(self):

		# Get the first job you find.
		if len(self.get_queued()) > 0:
			return self.get_queued()[0]
		else:
			return None

#
# Job
#
# A Job is an executable and any files it needs to be
# run against. A job contains one or more WorkUnits.
#
# PENDING = Job is created but may still be modified
# READY = Job is ready to be executed, added to queue
# RUNNING = Job has at least one work unit being run
# FINISHED = Job has all work units finished
#

class Job(object):

	def __init__(self, job_id, executable, flags, wall_time, deadline, budget):
		self.job_id = job_id
		self.executable = executable
		self.status = "PENDING"
		self.wall_time = wall_time
		self.deadline = deadline
		self.flags = flags
		self.budget = budget

		self.created_ts = int(time.time())
		self.ready_ts = None
		self.running_ts = None
		self.finished_ts = None
		
		self.files = []
		self.work_units = []

	#
	# @property budget_per_node_hour(self)
	# 
	# The budget available per node per hour is the overall budget
	# divided by how many cores are required (number of work units)
	# which determins how much money is available per node. Then
	# this is divided by the wall_time, which is how many hours are
	# required per node.
	#

	@property
	def budget_per_node_hour(self):
		return int(self.budget) / self.num_work_units / time.strptime(self.wall_time, "%H:%M:%S").tm_hour

	@property
	def num_work_units(self):
		return len(self.work_units)

	# 
	# Status Setters
	#

	def running(self):
		self.status = "RUNNING"
		self.running_ts = int(time.time())

	def ready(self):
		self.status = "READY"
		self.ready_ts = int(time.time())

		self.create_work_units()

	def finish(self):
		self.status = "FINISHED"
		self.finished_ts = int(time.time())

	#
	# Status Checkers
	#

	def is_running(self):
		return self.status == "RUNNING"

	def is_finished(self):

		# Assume the job is finished. Look for contradiction.
		finished = True
		for work_unit in self.work_units:
			if work_unit.status != "FINISHED":
				finished = False
				break

		return finished

	#
	#
	#
	
	def create_file_path(self, file_path):
		file_dir = os.path.join("jobs", str(self.job_id), "files")
		path = os.path.join( file_dir, os.path.dirname(file_path) )

		if not os.path.exists(path):
			path = os.makedirs(path)

		return os.path.join( file_dir, file_path ) 

	def add_file(self, filename):
		self.files.append(filename)

	def create_work_units(self):
		if self.files:
			for filename in self.files:
				self.work_units.append( WorkUnit(self, filename) )
		else:
			self.work_units.append( WorkUnit(self) )

	def finish_work_unit(self, filename):
		for unit in self.work_units:
			if unit.filename == filename:
				unit.finished()
				return unit

	#
	# Representations
	#

	def to_dict(self):
		d = {
			'job_id': self.job_id,
			'executable': self.executable,
			'files': self.files,
			'status': self.status,
			'walltime': self.wall_time,
			'deadline': self.deadline,
			'flags': self.flags,
			'budget': self.budget,
			'created_ts': self.created_ts,
			'ready_ts': self.ready_ts,
			'running_ts': self.running_ts,
			'finished_ts': self.finished_ts,
			'work_units': [],
		}

		for work_unit in self.work_units:
			d['work_units'].append(work_unit.to_dict())
		
		return d
	
	def to_json(self):
		return json.dumps(self.to_dict())

	def __str__(self):
		return self.to_json()

	def __repr__(self):
		return self.to_json()

#
# WorkUnit
#
# A WorkUnit is what is sent to each Node for processing 
# each WorkUnit is to be scheduled independently.
#

class WorkUnit(object):
	
	def __init__(self, job, filename = None):
		self.job = job
		self.process_id = None
		self.node_id = None
		self.status = "QUEUED"
		self.filename = filename
		self.created_ts = int(time.time())
		self.finished_ts = None

	@property
	def cost(self):
		return self.job.budget_per_node_hour

	@property
	def command(self):
		return "%s %s" % (self.job.executable, self.job.flags)

	def running(self, node_id):
		self.node_id = node_id
		self.status = "RUNNING"
		
		if not self.job.is_running():
			self.job.running()

	def finished(self):
		self.status = "FINISHED"
		self.finished_ts = int(time.time())
		
		if self.job.is_finished():
			self.job.finish()			

	def to_dict(self):
		d = {
			'job_id': self.job.job_id,
			'executable': self.job.executable,
			'flags': self.job.flags,
			'filename': self.filename,
			'wall_time': self.job.wall_time,

			'node_id': self.node_id,
			'status': self.status,
			'cost': self.cost,
			'created_ts': self.created_ts,
			'finished_ts': self.finished_ts,
		}	

		return d

	def to_json(self):
		return json.dumps(self.to_dict())

	def __str__(self):
		return self.to_json()

	def __repr__(self):
		return self.to_json()

#
# NodeNotFoundException
#

class NodeNotFoundException(Exception):
	pass

#
# JobNotFoundException
#

class JobNotFoundException(Exception):
	pass

#
# InvalidJobStatusException
#

class InvalidJobStatusException(Exception):
	pass
