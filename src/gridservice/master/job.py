import os
import time
import json

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

	def ready(self):
		self.status = "READY"
		self.ready_ts = int(time.time())

		self.create_work_units()

	def running(self):
		self.status = "RUNNING"
		self.running_ts = int(time.time())

	def finish(self):
		self.status = "FINISHED"
		self.finished_ts = int(time.time())

	def kill(self):
		self.status = "KILLED"
		self.finished_ts = int(time.time())

		for unit in self.work_units:
			unit.kill()

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
	
	@property
	def input_dir(self):
		return os.path.join("www", "jobs", str(self.job_id), "input")

	@property
	def output_dir(self):
		return os.path.join("www", "jobs", str(self.job_id), "output")

	def input_path(self, filename):
		return os.path.join(self.input_dir, filename)

	def output_path(self, filename):
		return os.path.join(self.output_dir, filename)

	def create_file_paths(self):
		self.create_file_path(self.output_dir)

	def create_file_path(self, file_path):
		dir_path = os.path.dirname(file_path)
		if not os.path.exists(dir_path):
			os.makedirs(dir_path)

	def add_file(self, filename):
		self.files.append(filename)

	def create_work_units(self):
		if self.files:
			for i, filename in enumerate(self.files):
				self.work_units.append( WorkUnit(i, self, filename) )
		else:
			self.work_units.append( WorkUnit(0, self) )

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
	
	def __init__(self, work_unit_id, job, filename = None):
		self.job = job
		
		self.work_unit_id = work_unit_id
		self.task_id = None
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

	def running(self, node_id, task_id):
		self.node_id = node_id
		self.task_id = task_id
		self.status = "RUNNING"
		
		if not self.job.is_running():
			self.job.running()

	def finished(self):
		self.status = "FINISHED"
		self.finished_ts = int(time.time())
		
		if self.job.is_finished():
			self.job.finish()			

	def kill(self):
		self.status = "KILLED"
		self.finished_ts = int(time.time())

	def to_dict(self):
		d = {
			'work_unit_id': self.work_unit_id,
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

