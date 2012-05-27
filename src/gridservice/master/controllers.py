import os
import time

import gridservice.utils
import gridservice.master.model as model

from gridservice import http
from gridservice.utils import validate_request
from gridservice.http import require_json, authenticate, FileResponse, JSONResponse
from gridservice.master.grid import NodeNotFoundException, JobNotFoundException, InvalidJobStatusException, InvalidSchedulerException, InvalidJobTypeException
from gridservice.master.scheduler import NodeUnavailableException

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
		['executable', 'wall_time', 'deadline', 'flags', 'budget', 'job_type']):
		return JSONResponse({ 'error_msg': 'Invalid Job JSON received.' }, http.BAD_REQUEST)
	try:
		job = model.grid.add_job(
			executable = d['executable'], 
			flags = d['flags'], 
			wall_time = d['wall_time'], 
			deadline = d['deadline'], 
			budget = d['budget'],
			job_type = d['job_type']
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
		file_path = joib.output_path(v['path'])
	else:
		return JSONResponse({ 'error_msg': "Invalid file type." }, http.BAD_REQUEST)

	return FileResponse(file_path)

#
# job_files_PUT(request, v)
# 
# Takes a binary PUT to a path and stores the file on 
# the local disk based on the id and path of the file
#

@auth_node
def job_files_PUT(request, v):
	try:
		job = model.grid.get_job(v['id'])
	except JobNotFoundException as e:
		return JSONResponse({ 'error_msg': e.args[0] }, http.NOT_FOUND)

	if v['type'] == "files":
		file_path = job.input_path(v['path'])
	elif v['type'] == "output":
		file_path = job.output_path(v['path'])
	else:
		return JSONResponse({ 'error_msg': "Invalid file type." }, http.BAD_REQUEST)

	job.create_file_path(file_path)
	request.raw_to_file(file_path)
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
	if not validate_request(request.json, ['filename']): 
		return JSONResponse({ 'error_msg': 'Invalid Work Unit JSON received.' }, http.BAD_REQUEST)

	try:
		job = model.grid.get_job(v['id'])
	except JobNotFoundException as e:
		return JSONResponse({ 'error_msg': e.args[0] }, http.NOT_FOUND)

	unit = model.grid.finish_work_unit(job, request.json['filename'])

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
