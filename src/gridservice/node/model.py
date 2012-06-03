import time
import sys
import os
import subprocess
import shlex
import shutil
import multiprocessing
import thread

from threading import Thread
from urllib2 import HTTPError, URLError
from httplib import HTTPException

import gridservice.node.monitor as monitor
import gridservice.node.utils as node_utils

from gridservice.http import auth_header, HTTPRequest, FileHTTPRequest, JSONHTTPRequest
import gridservice.walltime as walltime

SERVERS = [
	('server', 'server')
]

class NodeServer(object):
	
	RETRY_MAX_ATTEMPTS = 5
	RETRY_INTERVAL = 1
	HEARTBEAT_INTERVAL = 5
	MONITOR_INTERVAL = 1

	def __init__(self, username, password, host, port, ghost, gport, cost, cores, programs):

		self.username = username
		self.password = password

		self.host = host
		self.port = port
		self.ghost = ghost
		self.gport = gport

		self.tasks = {}
		self.next_task_id = 0
		self.retry_attempts = 0

		self.programs = programs
		self.cost = int(cost)

		self.auth_header = auth_header(self.username, self.password)
	
		if cores <= 0:
			try:
				self.cores = multiprocessing.cpu_count()
			except NotImplementedError:
				self.cores = 1
		else:
			self.cores = int(cores)
		
		# Register the node with The Grid
		try:
			self.node_id = self.register_node()
		except ServerUnavailableException as e:
			print "%s" % (e.args[0])
			sys.exit(1)

		# Start the Monitor
		self.mon = monitor.Monitor()

		# Start the Process Monitor and Heartbeat
		self.start_monitor()
		self.start_heartbeat()

	#
	# @property grid_url
	#
	# Utility property to easily build the grid url
	#

	@property
	def grid_url(self):
		return "http://%s:%s" % (self.ghost, self.gport)

	#
	# reset_node_state(self)
	#
	# In the case of Server failure, all tasks on the Node
	# become invalid. Kills off all running tasks, and
	# resets the state of the node.
	#

	def reset_node_state(self):
		print "Reconnected to The Grid. Resetting."
		# Kill all active tasks
		for task in self.tasks.values():
			task.kill()
		
		# Remove all task related files
		path = os.path.join('www', 'tasks')
		if os.path.exists(path):
			shutil.rmtree(path)

		# Reset internal state
		self.tasks = {}
		self.next_task_id = 0
		self.retry_attempts = 0

	#
	# register_node(self)
	#
	# Informs the server of the node's existence. Returns
	# the node ID assigned to it by the server.
	#

	def register_node(self):
		try:
			request = JSONHTTPRequest( 'POST', self.grid_url + '/node', { 
				'host': self.host,
				'port': self.port,
				'cores': self.cores,
				'programs': self.programs,
				'cost': self.cost,
			}, self.auth_header)
		except (HTTPException, URLError) as e:
			node_utils.request_error_cli(e, "Unable to establish a connection to The Grid")
			raise ServerUnavailableException("The Grid is currently unavailable.")

		# Reset the node state in the event of Server failure
		self.reset_node_state()

		return request.response['node_id']

			
	#
	# add_task(self, job_id, work_unit_id, executable, filename, flags, wall_time, deadline)
	#
	# Takes the given task variables and creates a new task, requests the
	# required file from the server, and readies the task for execution, which
	# in turn executes the task.
	#

	def add_task(self, job_id, work_unit_id, executable, filename, flags, wall_time, deadline):
		# create the task
		task = Task(
			task_id = self.next_task_id, 
			job_id = job_id,
			work_unit_id = work_unit_id,
			executable = executable, 
			filename = filename,
			flags = flags, 
			wall_time = walltime.strptime(wall_time),
			deadline = deadline
		)

		# Get the files for the task
		self.get_task_executable(task)
		self.get_task_file(task)

		# Task is now READY
		task.ready()
	
		self.tasks.update({ task.task_id: task })
		self.next_task_id += 1

		return task
	
	#
	# get_task_executable(self, task)
	#
	# Requests the executable file required by the given task from the
	# server and saves the file to disk
	#

	def get_task_executable(self, task):
		try:
			url = "%s/job/%s/executable/%s" % (self.grid_url, task.job_id, task.executable)
			request = HTTPRequest( 'GET', url, "", self.auth_header)
		except(HTTPException, URLError) as e:
			node_utils.request_error_cli(e, "Unable to establish a connection to The Grid")
			sys.exit(1)

		# This will be horrible with large files

		fp = open(task.executable_path, 'w+')
		fp.write(request.response)
		fp.close()

	#
	# get_task_file(self, task)
	#
	# Requests the file required by the given task from the 
	# server and saves the file to disk
	#

	def get_task_file(self, task):
		try:
			url = "%s/job/%s/files/%s" % (self.grid_url, task.job_id, task.filename)
			request = HTTPRequest( 'GET', url, "", self.auth_header)
		except (HTTPException, URLError) as e:
			node_utils.request_error_cli(e, "Unable to establish a connection to The Grid")
			sys.exit(1)
	
		# This will be horrible with large files

		fp = open(task.input_path, 'w+')
		fp.write(request.response)
		fp.close()

	#
	# get_task(self, task_id)
	#
	# Get the task of the given id
	#

	def get_task(self, task_id):
		if isinstance(task_id, str) and task_id.isdigit():
			task_id = int(task_id)

		if task_id in self.tasks:
			return self.tasks[ task_id ]
		else:
			raise TaskNotFoundException("There is no task with the id: %s" % task_id)

	#
	# send_task_output(self, task)
	#
	# Sends the output and error files of a task to the sever.
	#

	def send_task_output(self, task):
	
		# Send the results of stdout
		try:
			url = '%s/job/%s/output/%s' % (self.grid_url, str(task.job_id), task.output_name + ".o")
			request = FileHTTPRequest( 'PUT', url, task.output_path, self.auth_header )
		except (HTTPException, URLError) as e:
			node_utils.request_error_cli(e, "Unable to establish a connection to the grid")

		# Send the results of stderr
		try:
			url = '%s/job/%s/output/%s' % (self.grid_url, str(task.job_id), task.output_name + ".e")
			request = FileHTTPRequest( 'PUT', url, task.error_path, self.auth_header )
		except (HTTPException, URLError) as e:
			node_utils.request_error_cli(e, "Unable to establish a connection to the grid")

	#
	# cleanup_task_files(self, task)
	#
	# Removes the files and directories on the node for the specified task
	#

	def cleanup_task_files(self, task):
		cwd = os.getcwd()
		os.chdir(os.path.join("www", "tasks"))
		subprocess.Popen(['rm', '-rf', "%s/" % task.task_id])
		os.chdir(cwd)
	
	#
	# kill_task(self, task, kill_msg)
	#
	# Kills a running process and forces the immediate return
	# of any output files created. Sends back reason for kill 
	# to The Grid.
	#

	def kill_task(self, task, kill_msg=None):
		task.outfile.close()
		task.errfile.close()

		task.kill()
		self.finish_task(task, kill_msg)
		del self.tasks[task.task_id]
	#
	# finish_task(self, task)
	#
	# Send the .o and .e files from the process to the server
	# and inform the server the task is completed.
	# Sends back the kill_msg which will be None unless the Node
	# killed the task.
	#

	def finish_task(self, task, kill_msg = None):

		self.send_task_output(task)
		self.cleanup_task_files(task)

		# Inform the server the task is complete
		try:
			url = '%s/job/%s/workunit' % (self.grid_url, str(task.job_id))
			request = JSONHTTPRequest( 'POST', url, { 
				'work_unit_id': task.work_unit_id,
				'kill_msg': kill_msg,
			}, self.auth_header)
		except (HTTPException, URLError) as e:
			node_utils.request_error_cli(e, "Unable to establish a connection to the grid")

		# Update the task internally to reflect that the server has 
		# received all files and the complete status.
		task.finish()

	#
	# Heartbeat
	#

	def start_heartbeat(self):
		self.heartbeat_thread = Thread(target = self.heartbeat)
		self.heartbeat_thread.name = "Node:Heartbeat"
		self.heartbeat_thread.daemon = True
		self.heartbeat_thread.start()

	#
	# heartbeat(self)
	#
	# Send the Node heartbeat to The Grid every HEARTBEAT_INTERVAL
	# seconds. If the heartbeat fails to be sent RETRY_MAX_ATTEMPTS
	# in a row, the Node will exit. 
	#

	def heartbeat(self):
		while True:
			try:
				url = '%s/node/%s' % (self.grid_url, str(self.node_id))
				request = JSONHTTPRequest( 'POST', url, { 
					'cpu': self.mon.cpu(),
				}, self.auth_header)
			except (HTTPException, URLError) as e:
				
				node_utils.request_error_cli(e, 
					"Heatbeat Failed: Unable to establish a connection to the grid")

				if self.retry_attempts < self.RETRY_MAX_ATTEMPTS:
					try:
						self.node_id = self.register_node()
					except ServerUnavailableException as e:
						self.retry_attempts += 1
				else:	
					print "Unable to connect to The Grid after %d unsuccessful attempts" % (self.RETRY_MAX_ATTEMPTS)

					# Reset the node state to clean up any running tasks
					self.reset_node_state()

					thread.interrupt_main()

			time.sleep(self.HEARTBEAT_INTERVAL)

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
	
	#
	# monitor_tasks(self)
	#
	# Monitors the tasks and checks if they are finished,
	# or kills them if they exceed their wall time or deadline.
	#

	def monitor_tasks(self):
		print self.tasks
		
		for i, task in list(self.tasks.items()):
			# Check if a task has finished
			if task.has_finished():
				self.finish_task(task)
				del self.tasks[i]

			# Kill task if its exceeded it wall time
			elif (int(time.time()) - task.running_ts) > walltime.wall_secs(task.wall_time):
				self.kill_task(task, "Exceeded Wall time.")
				print "Work unit %s of job %s killed: Exceeded Wall Time." % (task.job_id, task.work_unit_id)

			# Kill task if it exceeds its deadline (for fairness)
			elif int(time.time()) > task.deadline:
				self.kill_task(task, "Exceeded deadline.")
				print "Work unit %s of job %s killed: Exceeded Deadline." % (task.job_id, task.work_unit_id)

#
# Task
#
# A task is the Node's representation of a Work Unit from the server.
#
# PENDING = Task is created but may still be modified
# READY = Task is ready to be executed, added to queue
# RUNNING = Task has been executed and is still running, or 
# has finished but has not yet been updated by the task monitor
# FINISHED = The Task has finished executing
#

class Task(object):
	
	def __init__(self, task_id, job_id, work_unit_id, executable, filename, flags, wall_time, deadline):
		self.task_id = task_id

		self.job_id = job_id
		self.work_unit_id = work_unit_id
		self.status = "PENDING"
		self.executable = executable
		self.flags = flags
		self.filename = filename
		self.wall_time = wall_time
		self.deadline = deadline

		self.created_ts = int(time.time())
		self.ready_ts = None
		self.running_ts = None
		self.finished_ts = None

		self.process = None
		self.infile = None
		self.outfile = None
		self.errfile = None

		self.create_file_paths()

	@property
	def command(self):
		return "./%s %s" % (self.executable_path, self.flags)
	
	def ready(self):
		self.status = "READY"
		self.ready_ts = int(time.time())

		self.execute()

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
		if self.is_running() and (self.process == None or self.process.poll() != None):
			return True
		else:
			return False

	@property
	def output_name(self):
		return "%s_%s" % (self.job_id, self.work_unit_id)

	@property
	def input_dir(self):
		return os.path.join("www", "tasks", str(self.task_id), "input")

	@property
	def output_dir(self):
		return os.path.join("www", "tasks", str(self.task_id), "output")

	@property
	def executable_dir(self):
		return os.path.join("www", "tasks", str(self.task_id), "executable")

	@property
	def input_path(self):
		return os.path.join(self.input_dir, self.filename)

	@property
	def output_path(self):
		return os.path.join(self.output_dir, self.output_name + ".o")
	
	@property
	def error_path(self):
		return os.path.join(self.output_dir, self.output_name + ".e")

	@property
	def executable_path(self):
		return os.path.join(self.executable_dir, self.executable)

	def create_file_paths(self):
		self.create_file_path(self.input_path)
		self.create_file_path(self.output_path)
		self.create_file_path(self.executable_path)

	def create_file_path(self, file_path):
		dir_path = os.path.dirname(file_path)
		if not os.path.exists(dir_path):
			os.makedirs(dir_path)

	def kill(self):
		# Kill the running process
		if self.is_running() and not self.has_finished():
			self.process.kill()

	def execute(self):
		if not os.path.exists(self.executable_path):
			raise ExecutableNotFoundException("Executable %s not found." % self.executable)

		if self.filename:
			if not os.path.exists(self.input_path):
				raise InputFileNotFoundException("Input file %s not found." % self.filename)
			else:
				self.infile = open(self.input_path, "r+")

		self.outfile = open(self.output_path, "w+")
		self.errfile = open(self.error_path, "w+")
		
		# Change the permissions of the executable file to allow execution
		subprocess.Popen(['chmod', '775', self.executable_path])

		# A bug in shlex causes it to spaz out on non-ascii strings
		# in Python 2.6, so we convert the string to ascii and ignore
		# any special unicode characters that might be in the command
		command = self.command.encode('ascii', 'ignore')
		args = shlex.split(command)
		
		try:
			self.process = subprocess.Popen(args, 
				stdout = self.outfile, stderr = self.errfile, stdin = self.infile)
		except OSError as e:
			self.errfile.write("Error: Provided Executable crashed at runtime.\n")
			self.errfile.close()
			self.outfile.close()
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

#
# ServerUnavailableException
#

class ServerUnavailableException(Exception):
	pass
