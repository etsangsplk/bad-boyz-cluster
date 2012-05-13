import time
import sys
import os
import subprocess
import shlex
import multiprocessing
from threading import Thread

import gridservice.node.monitor as monitor
import gridservice.node.utils as node_utils

from gridservice.http import JSONHTTPRequest, JSONResponse
from gridservice.grid import WorkUnit 

from urllib2 import HTTPError, URLError
from httplib import HTTPException

class NodeServer:
	
	HEARTBEAT_INTERVAL = 5
	MONITOR_INTERVAL = 1

	def __init__(self, host, port, ghost, gport):

		self.host = host
		self.port = port
		self.ghost = ghost
		self.gport = gport

		self.tasks = []
		self.next_task_id = 0

		cores = None
	
		if not cores:
			try:
				self.cores = multiprocessing.cpu_count()
			except NotImplementedError:
				self.cores = 1
		else:
			self.cores = cores

		self.programs = [ './test.py' ]
		self.cost = 15

		# Register the node with The Grid
		self.node_id = self.register_node()

		# Start the Monitor
		self.mon = monitor.Monitor()

		# Start the Process Monitor and Heartbeat
		self.start_monitor()
		self.start_heartbeat()

	@property
	def grid_url(self):
		return "http://%s:%s" % (self.ghost, self.gport)

	def register_node(self):
		try:
			request = JSONHTTPRequest( 'POST', self.grid_url + '/node', { 
				'host': self.host,
				'port': self.port,
				'cores': self.cores,
				'programs': self.programs,
				'cost': self.cost,
			})
		except (HTTPException, URLError) as e:
			node_utils.request_error_cli(e, "Unable to establish a connection to the grid")
			sys.exit(1)
	
		return request.response['node_id']

	def add_task(self, job_id, executable, flags, filename, wall_time):
		task = Task(
			task_id = self.next_task_id, 
			job_id = job_id,
			executable = executable, 
			flags = flags, 
			filename = filename, 
			wall_time = wall_time
		)
		self.tasks.append(task)
		self.next_task_id += 1

		return task

	def get_task(self, task_id):
		if isinstance(task_id, str) and task_id.isdigit():
			task_id = int(task_id)

		if task_id in self.tasks:
			return self.tasks[ task_id ]
		else:
			raise TaskNotFoundException("There is no task with the id: %s" % task_id)

	def update_task_status(self, task_id, status):
		if status not in [ "READY" ]:
			raise InvalidTaskStatusException("The task status %s is not valid." % status)
		
		if status == "READY":
			task.ready()
			task.execute()

		return task

	def finish_task(self, task):
		task.finish()

		url = self.grid_url + '/job/' + str(task.job_id) + '/workunit'
		request = JSONHTTPRequest( 'POST', url, { 
			'job_id': task.job_id,
			'filename': task.filename,
		})

	#
	# Heartbeat
	#

	def start_heartbeat(self):
		self.heartbeat_thread = Thread(target = self.heartbeat)
		self.heartbeat_thread.name = "Node:Heartbeat"
		self.heartbeat_thread.daemon = True
		self.heartbeat_thread.start()

	def heartbeat(self):
		while True:
			self.send_heartbeat()
			time.sleep(self.HEARTBEAT_INTERVAL)

	def send_heartbeat(self):
		try:
			request = JSONHTTPRequest( 'POST', self.grid_url + '/node/' + str(self.node_id), { 
				'cpu': self.mon.cpu(),
			})
			print "Heartbeat: (CPU: " + str(self.mon.cpu()) + "%)"

		except (HTTPException, URLError) as e:
			node_utils.request_error_cli(e, "Heatbeat Failed: Unable to establish a connection to the grid")

	#
	# Process Monitor
	#

	def start_monitor(self):
		self.monitor_thread = Thread(target = self.monitor)
		self.monitor_thread.name = "Node:Monitor"
		self.monitor_thread.daemon = True
		self.monitor_thread.start()

	def monitor(self):
		while True:
			self.monitor_tasks()
			time.sleep(self.MONITOR_INTERVAL)
	
	def monitor_tasks(self):
		print self.tasks
		for i, task in enumerate(self.tasks):
			if task.has_finished():
				self.finish_task(task)
				del self.tasks[i]

#
# Task
#
# A task is the Node's representation of a Work Unit from the server.
#

class Task:
	
	def __init__(self, task_id, job_id, executable, flags, filename, wall_time):
		self.task_id = task_id

		self.job_id = job_id
		self.status = "PENDING"
		self.executable = executable
		self.flags = flags
		self.filename = filename
		self.wall_time = wall_time

		self.created_ts = int(time.time())
		self.ready_ts = None
		self.running_ts = None
		self.finished_ts = None
	
		self.process = None
		self.infile = None
		self.outfile = None
		self.errfile = None

	@property
	def command(self):
		return "%s %s" % (self.executable, self.flags)

	def ready(self):
		self.status = "READY"
		self.ready_ts = int(time.time())

	def running(self):
		self.status = "RUNNING"
		self.running_ts = int(time.time())

	def finish(self):
		self.status = "FINISHED"
		self.finished_ts = int(time.time())

	def is_ready(self):
		return self.status == "READY"

	def is_running(self):
		return self.status == "RUNNING"
	
	def is_finished(self):
		return self.status == "FINISHED"

	def has_finished(self):
		if self.is_running() and self.process.poll() != None:
			return True
		else:
			return False

	def execute(self):
		if not os.path.exists(self.executable):
			raise ExecutableNotFoundException("Executable %s not found." % self.executable)

		if self.filename:
			if not os.path.exists(self.filename):
				raise InputFileNotFoundException("Input file %s not found." % self.filename)
			else:
				self.infile = open(self.filename, "r+")

			executable_name = os.path.basename(self.executable)
			filename_name = os.path.basename(self.filename)

			prefix = executable_name + "_" + filename_name

		self.outfile = open(prefix + ".o", "w+")
		self.errfile = open(prefix + ".e", "w+")

		# A bug in shlex causes it to spaz out on non-ascii strings
		# in Python 2.6, so we convert the string to ascii and ignore
		# any special unicode characters that might be in the command

		command = self.command.encode('ascii', 'ignore')
		args = shlex.split(command)

		self.process = subprocess.Popen(args, 
			stdout = self.outfile, stderr = self.errfile, stdin = self.infile)

		self.running()

	def __repr__(self):
		return "%s (%s) - %s: %s < %s" % (self.task_id, self.status, self.job_id, self.command, self.filename)

#
# ExecutableNotFoundException
#

class ExecutableNotFoundException(Exception):
	pass

#
# InputFileNotFoundException
#

class InputFileNotFoundException(Exception):
	pass

#
# InvalidTaskStatusException
#

class InvalidTaskStatusException(Exception):
	pass

#
# TaskNotFoundException
#

class TaskNotFoundException(Exception):
	pass

