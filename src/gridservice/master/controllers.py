import os
import time

import gridservice.utils
import gridservice.master.model as model

from gridservice import http
from gridservice.utils import validate_request
from gridservice.http import require_json, authenticate, FileResponse, JSONResponse
from gridservice.master.grid import NodeNotFoundException, JobNotFoundException, InvalidJobStatusException, InvalidSchedulerException, InvalidJobTypeException
from gridservice.master.scheduler import NodeUnavailableException

def auth_any(func):
	def decorator_func(*args, **kwargs):
		return authenticate(model.ADMINS + model.CLIENTS + model.NODES)(func)(*args, **kwargs)
	return decorator_func

def auth_admin(func):
	def decorator_func(*args, **kwargs):
		return authenticate(model.ADMINS)(func)(*args, **kwargs)
	return decorator_func

def auth_client(func):
	def decorator_func(*args, **kwargs):
		return authenticate(model.ADMINS + model.CLIENTS)(func)(*args, **kwargs)
	return decorator_func

def auth_node(func):
	def decorator_func(*args, **kwargs):
		return authenticate(model.ADMINS + model.NODES)(func)(*args, **kwargs)
	return decorator_func

#
# schduler_PUT
#
# Sets the scheduler in use by The Grid
#

@require_json
@auth_admin
def scheduler_PUT(request):
	d = request.json

	if not validate_request(d, ['scheduler']):
		return JSONResponse({ 'error_msg': 'Invalid Scheduler JSON Received' }, http.BAD_REQUEST)
	
	try:
		model.grid.scheduler = request.json['scheduler']
	except InvalidSchedulerException as e:
		return JSONResponse({ 
			'error_msg': "Invalid Scheduler %s. Valid Schedulers: %s" %
			(request.json['scheduler'], ", ".join(model.grid.SCHEDULERS))
		}, 
		http.BAD_REQUEST)
	
	return JSONResponse({ 'success': 'Scheduler changed.' }, http.OK)

#
# job_GET(request)
#
# Returns a list of all jobs
#

@auth_client
def job_GET(request):	
	jobs = model.grid.jobs
	
	safe_jobs = {}
	for key, job in jobs.items():
		safe_jobs.update({ key: job.to_dict() })
	
	return JSONResponse(safe_jobs, http.OK)

#
# job_POST(request)
#
# Creates a new Job sent by a client 
#

@require_json
@auth_client
def job_POST(request):
	d = request.json
	if not validate_request(d, 
		['wall_time', 'deadline', 'flags', 'budget', 'job_type']):
		return JSONResponse({ 'error_msg': 'Invalid Job JSON received.' }, http.BAD_REQUEST)
	try:
		name = "Unknown"
		if d.has_key("name"):
			name = d["name"]

		job = model.grid.add_job(
			flags = d['flags'], 
			wall_time = d['wall_time'], 
			deadline = d['deadline'], 
			budget = d['budget'],
			job_type = d['job_type'],
			name = name
		)
	except InvalidJobTypeException as e:
		return JSONResponse({ 
			'error_msg': "Invalid Job Type %s. Valid Job Types: %s" %
			(d['job_type'], ", ".join(model.grid.node_queue.keys()))
		}, 
		http.BAD_REQUEST)
		pass




	return JSONResponse({ 'success': "Job added successfully.", 'id': job.job_id }, http.OK)

#
# job_id_GET(request, v)
#
# Get a job by the id in the URI
#

@auth_client
def job_id_GET(request, v):
	try:
		job = model.grid.get_job(v['id'])
	except JobNotFoundException as e:
		return JSONResponse({ 'error_msg': e.args[0] }, http.NOT_FOUND)
	
	return JSONResponse(job.to_dict(), http.OK)

#
# job_id_DELETE(request, v)
#
# Kills the job running with ID
#

@auth_client
def job_id_DELETE(request, v):
	try:
		job = model.grid.get_job(v['id'])
	except JobNotFoundException as e:
		return JSONResponse({ 'error_msg': e.args[0] }, http.NOT_FOUND)
	
	try:
		model.grid.kill_job(job)
	except NodeUnavailableException as e:
		return JSONResponse({ 'error_msg': e.args[0] }, http.BAD_REQUEST)

	return JSONResponse({ 'success': "Job killed successfully." }, http.OK)

#
# job_status_PUT(request, v)
#
# Sets the status of the job by the id in the URI
#

@require_json
@auth_client
def job_status_PUT(request, v):
	d = request.json

	if not validate_request(d, ['status']): 
		return JSONResponse({ 'error_msg': 'Invalid status JSON received.' }, http.BAD_REQUEST)

	try:
		job = model.grid.update_job_status(v['id'], d['status'])
	except JobNotFoundException as e:
		return JSONResponse({ 'error_msg': e.args[0] }, http.NOT_FOUND)
	except InvalidJobStatusException as e:
		return JSONResponse({ 'error_msg': e.args[0] }, http.BAD_REQUEST)

	return JSONResponse(job.to_dict(), http.OK)

#
# job_status_GET(request, v)
#
# Gets the status of a job
#

@auth_client
def job_status_GET(request, v):
	try:
		job = model.grid.get_job(v['id'])
		return JSONResponse({ 'job_status': job.status }, http.OK)
	except JobNotFoundException as e:
		return JSONResponse({ 'error_msg': e.args[0] }, http.NOT_FOUND)
#
# job_output_files_GET(request, v)
# 
# Returns a list of filenames of a finished job
#

@auth_client
def job_output_files_GET(request, v):
	try:
		job = model.grid.get_job(v['id'])
	except JobNotFoundException as e:
		return JSONResponse({ 'error_msg': e.args[0] }, http.NOT_FOUND)

	if job.status != "FINISHED" and job.status != "KILLED":
		return JSONResponse({ 'error_msg': "Job %s has not finished running." % v['id']}, http.BAD_REQUEST)

	files_list = []
	for unit in job.work_units:
		files_list.append("%s_%s.o" % (job.job_id, unit.work_unit_id))
		files_list.append("%s_%s.e" % (job.job_id, unit.work_unit_id))
	return JSONResponse({ 'output_URIs': files_list }, http.OK)

#
# job_output_file_GET(request, v)
# 
# Returns a FileResponse of the file at the given URI
#

@auth_client
def job_output_file_GET(request, v):
	try:
		job = model.grid.get_job(v['id'])
	except JobNotFoundException as e:
		return JSONResponse({ 'error_msg': e.args[0] }, http.NOT_FOUND)

	if job.status != "FINISHED" and job.status != "KILLED":
		return JSONResponse({ 'error_msg': "Job %s has not finished running." % v['id']}, http.BAD_REQUEST)
	
	file_path = "%s/%s" % (job.output_dir, v['file_name'])

	try:
		f = open(file_path, "r")
		f.close()
	except IOError as e:
		return JSONResponse({ 'error_msg': "Unable to open file %s for Job %s." % (v['file_name'], v['id'])}, http.BAD_REQUEST)

	return FileResponse(file_path)

#
# job_files_GET(request, v)
# 
# Returns a FileResponse of the file at the given URI
#

@auth_node
def job_files_GET(request, v):
	try:
		job = model.grid.get_job(v['id'])
	except JobNotFoundException as e:
		return JSONResponse({ 'error_msg': e.args[0] }, http.NOT_FOUND)

	if v['type'] == "files":
		file_path = job.input_path(v['path'])
	elif v['type'] == "output":
		file_path = job.output_path(v['path'])
	elif v['type'] == "executable":
		file_path = job.executable_path(v['path'])
	else:
		return JSONResponse({ 'error_msg': "Invalid file type." }, http.BAD_REQUEST)

	return FileResponse(file_path)

#
# job_files_PUT(request, v)
# 
# Takes a binary PUT to a path and stores the file on 
# the local disk based on the id and path of the file
#

@auth_any
def job_files_PUT(request, v):
	try:
		job = model.grid.get_job(v['id'])
	except JobNotFoundException as e:
		return JSONResponse({ 'error_msg': e.args[0] }, http.NOT_FOUND)

	if v['type'] == "files":
		file_path = job.input_path(v['path'])
	elif v['type'] == "output":
		file_path = job.output_path(v['path'])
	elif v['type'] == "executable":
		file_path = job.executable_path(v['path'])
	else:
		return JSONResponse({ 'error_msg': "Invalid file type." }, http.BAD_REQUEST)

	job.create_file_path(file_path)
	request.raw_to_file(file_path)
	
	if v['type'] == "executable":
		job.add_executable(v['path'])
	else:
		job.add_file(v['path'])
	
	return JSONResponse(v)

#
# job_workunit_POST(request, v)
# 
# Marks the given workunit as finished
#

@require_json
@auth_node
def job_workunit_POST(request, v):
	if not validate_request(request.json, ['work_unit_id']): 
		return JSONResponse({ 'error_msg': 'Invalid Work Unit JSON received.' }, http.BAD_REQUEST)

	try:
		job = model.grid.get_job(v['id'])
	except JobNotFoundException as e:
		return JSONResponse({ 'error_msg': e.args[0] }, http.NOT_FOUND)

	unit = model.grid.finish_work_unit(job, request.json['work_unit_id'])

	return JSONResponse(unit.to_dict(), http.OK)

#
# node_GET(request)
#
# Get a list of all nodes
#

@auth_admin
def node_GET(request):
	nodes = model.grid.nodes

	safe_nodes = {}
	for key, node in nodes.items():
		safe_nodes.update({ key: model.grid.node_to_dict(node) })
	
	return JSONResponse(safe_nodes, http.OK)

#
# node_POST(request)
#
# Takes a ip_address, port and cores and initiates
# the new node in the network.
#

@require_json
@auth_node
def node_POST(request):
	if not validate_request(request.json, ['host', 'port', 'cores', 'programs', 'cost']): 
		return JSONResponse({ 'error_msg': 'Invalid Node JSON received.' }, http.BAD_REQUEST)
	
	node = request.json
	node_id = model.grid.add_node(node)

	return JSONResponse({ 'node_id': node_id }, http.OK)

#
# node_id_GET(request, v)
#
# Returns the node at the given URI
#

@auth_node
def node_id_GET(request, v):
	try:
		node = model.grid.get_node(v['id'])
	except NodeNotFoundException as e:
		return JSONResponse({ 'error_msg': e.args[0] }, http.NOT_FOUND)

	return JSONResponse(model.grid.node_to_dict(node), http.OK)

#
# node_id_POST(request, v)
#
# Updates the node at the given URI, returns the node
#

@require_json
@auth_node
def node_id_POST(request, v):
	if not validate_request(request.json, []): 
		return JSONResponse({ 'error_msg': 'Invalid Node JSON received.' }, http.BAD_REQUEST)

	try:
		node = model.grid.update_node(v['id'], request.json)
	except NodeNotFoundException as e:
		return JSONResponse({ 'error_msg': e.args[0] }, http.NOT_FOUND)

	return JSONResponse(model.grid.node_to_dict(node), http.OK)
		
#
# index_GET(request)
#
# A nice alias for the console index
#

def index_GET(request):
	return FileResponse(os.path.join("www", "console/console.html"))

#
# file_GET(request, v)
#
# Serves a file directly from disk
#

def file_GET(request, v):
	return FileResponse(os.path.join("www", v["file"]))


# Controller actions for the Console...
@auth_client
def nodes_GET(request):
	nodeList = model.grid.nodes.values()

	jsonNodes = []
	for node in nodeList:
		n = {
			"node_id": node['node_id'],
			"status": node['status'],
			"work_units": [],
			"type": node['type'],
			"node_ident": node['node_ident'],
			"cores": node['cores']
		}
		for unit in node["work_units"]:
			n["work_units"].append(unit.to_dict())
		
		jsonNodes.append(n)


	return  JSONResponse({ 'nodes': jsonNodes}, 200)


@auth_client
def jobs_GET(request):
	queued_jobs = model.grid.get_queued()
	queued_jobs = model.grid.jobs

	ljobs=[]
	for j in queued_jobs.values():
		ljobs.append(j.to_dict())

	# Now get running Jobs


	# And finally, completed jobs...


	return  JSONResponse({ 'jobs': ljobs}, 200)




def job_submit_file_POST(request, v):
	file_name = request.query["qqfile"][0]
	try:
		job = model.grid.get_job(v['tmp_job_id'])
	except JobNotFoundException as e:
		return JSONResponse({ 'error_msg': e.args[0] }, http.NOT_FOUND)


	file_path = job.input_path(file_name)

	job.create_file_path(file_path)
	request.raw_to_file(file_path)

	job.add_file(file_name)



	return JSONResponse( {'tmp_job_id': v['tmp_job_id'], 'filename': file_path} , 200)

def job_submit_executable_POST(request, v):
	file_name = request.query["qqfile"][0]
	try:
		job = model.grid.get_job(v['tmp_job_id'])
	except JobNotFoundException as e:
		return JSONResponse({ 'error_msg': e.args[0] }, http.NOT_FOUND)


	file_path = job.executable_path(file_name)
	
	job.create_file_path(file_path)
	request.raw_to_file(file_path)

	job.add_executable(file_name)

	return JSONResponse( {'tmp_job_id': v['tmp_job_id'], 'filename': file_path} , 200)


@auth_client
def job_queue_POST(request):
	if request.post.has_key("tmp_job_id"):
		tmp_job_id = int(request.post["tmp_job_id"][0])

	job_id = model.grid.tmp_job_enqueue(tmp_job_id)

	return JSONResponse( {'job_id': job_id} , 200)
