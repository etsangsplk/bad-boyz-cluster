import threading
import time 
import json
import copy
import os

from urllib2 import HTTPError, URLError
from httplib import HTTPException

from gridservice.http import JSONHTTPRequest
from gridservice.utils import validate_request

from gridservice.master.scheduler import BullshitScheduler
from gridservice.master.job import Job

#
# Grid
#
# If you are on the Grid, you can never leave.
#

class Grid(object):
	
	#
	# __init__(self, scheduler_func)
	#
	# Initialises The Grid using the given Scheduler
	#

	def __init__(self, scheduler_func):
		self.jobs = {}
		self.next_job_id = 0
		
		self.nodes = {}
		self.node_ids = {}
		self.next_node_id = 0

		self.queue_lock = threading.Lock()
		self.queue = []

		# Start the scheduler
		self.scheduler = scheduler_func(self)
		self.scheduler.start()

	#
	# add_job(self, executable, flags, wall_time, deadline, budget)
	#
	# Adds a new job to the Grid
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
	# Gets a job based on job ID
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
	# Updates a jobs status, currently only supports READY
	#

	def update_job_status(self, job_id, status):
		if status not in [ "READY" ]:
			raise InvalidJobStatusException("The job status %s is not valid." % status)

		job = self.get_job(job_id)
		
		if status == "READY":
			job.ready()
			self.add_to_queue(job)

		return job
		
	#
	# add_to_queue(self, job)
	#
	# Adds a job to the queue
	#

	def add_to_queue(self, job):
		with self.queue_lock:
			for work_unit in job.work_units:
				self.queue.append(work_unit)

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
	# get_node_ident(self, node)
	#
	# A utility function for easily getting a node's ident
	#

	def get_node_ident(self, node):
		return "%s:%s" % (node['host'], node['port'])

	#
	# get_node_url(self, node)
	#
	# A utility function for easily getting a node's url
	#

	def get_node_url(self, node):
		return "http://%s" % (self.get_node_ident(node))

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
	# get_queued(self)
	#
	# A generator of all queued work units
	# 

	def get_queued(self):
		return [unit for unit in self.queue if unit.status == "QUEUED"]

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
