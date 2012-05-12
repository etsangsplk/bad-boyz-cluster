import os
import time
import gridservice.utils
from gridservice import http
from gridservice.http import require_json, Response, FileResponse, JSONResponse
from gridservice.grid import Job, NodeNotFoundException, JobNotFoundException

import gridservice.master.model as model

#
# job_GET(request)
#
# Returns a list of all jobs
#

def job_GET(request):
	jobs = model.grid.jobs
	
	safe_jobs = {}
	for key, job in jobs.items():
		safe_jobs.update({ key: job.to_dict() })
	
	return JSONResponse(safe_jobs, 200)

#
# job_POST(request)
#
# Creates a new Job sent by a client and adds it 
# to the queue
#

@require_json
def job_POST(request):
	d = request.json

	if not gridservice.utils.validate_request(d, 
		['executable', 'wall_time', 'deadline', 'flags', 'budget']):
		return JSONResponse({ 'error_msg': 'Invalid Job JSON received.' }, http.BAD_REQUEST)

	job = model.grid.add_job(
		executable = d['executable'], 
		flags = d['flags'], 
		wall_time = d['wall_time'], 
		deadline = d['deadline'], 
		budget = d['budget']
	)

	return JSONResponse({ 'success': "Job added successfully.", 'id': job.job_id }, 200)

#
# job_id_GET(request, v)
#
# Get a job by the id in the URI
#

def job_id_GET(request, v):
	try:
		job = model.grid.get_job(v['id'])
	except JobNotFoundException as e:
		return JSONResponse({ 'error_msg': e.args[0] }, 404)
	
	return JSONResponse(job.to_dict(), 200)

#
# job_files_PUT(request, v)
# 
# Takes a binary PUT to a path and stores the file on 
# the local disk based on the id, type and path of the file
#

def job_files_PUT(request, v):
	file_dir = os.path.join("jobs", v['id'], "files")
	path = os.path.join( file_dir, os.path.dirname(v['path']) )

	if not os.path.exists(path):
		path = os.makedirs(path)

	request.raw_to_file( os.path.join(file_dir, v['path']) )

	return JSONResponse(v)

#
# job_workunit_POST(request, v)
# 
# 
#

@require_json
def job_workunit_POST(request, v):
	if not gridservice.utils.validate_request(request.json, ['filename']): 
		return JSONResponse({ 'error_msg': 'Invalid Work Unit JSON received.' }, http.BAD_REQUEST)

	try:
		job = model.grid.get_job(v['id'])
	except JobNotFoundException as e:
		return JSONResponse({ 'error_msg': e.args[0] }, 404)

	unit = job.finish_work_unit(request.json['filename'])

	return JSONResponse(unit.to_dict(), 200)

#
# node_GET(request)
#
# Get a list of all nodes
#

def node_GET(request):
	nodes = model.grid.nodes

	safe_nodes = {}
	for key, node in nodes.items():
		safe_nodes.update({ key: model.grid.node_to_dict(node) })
	
	return JSONResponse(safe_nodes, 200)

#
# node_POST(request)
#
# Takes a ip_address, port and cores and initiates
# the new node in the network.
#

@require_json
def node_POST(request):
	if not gridservice.utils.validate_request(request.json, ['host', 'port', 'cores', 'programs', 'cost']): 
		return JSONResponse({ 'error_msg': 'Invalid Node JSON received.' }, http.BAD_REQUEST)
	
	node = request.json
	node_id = model.grid.add_node(node)

	return JSONResponse({ 'node_id': node_id }, 200)

#
# node_id_GET(request, v)
#
# Returns the node at the given URI
#

def node_id_GET(request, v):
	try:
		node = model.grid.get_node(v['id'])
	except NodeNotFoundException as e:
		return JSONResponse({ 'error_msg': e.args[0] }, 404)

	return JSONResponse(model.grid.node_to_dict(node), 200)

#
# node_id_POST(request, v)
#
# Updates the node at the given URI, returns the node
#

@require_json
def node_id_POST(request, v):
	if not gridservice.utils.validate_request(request.json, []): 
		return JSONResponse({ 'error_msg': 'Invalid Node JSON received.' }, http.BAD_REQUEST)

	try:
		node = model.grid.update_node(v['id'], request.json)
	except NodeNotFoundException as e:
		return JSONResponse({ 'error_msg': e.args[0] }, 404)

	return JSONResponse(model.grid.node_to_dict(node), 200)
		
#
# index_GET(request)
#
# A nice alias for the console index
#

def index_GET(request):
	return FileResponse("console/console.html")

#
# file_GET(request, v)
#
# Serves a file directly from disk
#

def file_GET(request, v):
	return FileResponse(v["file"])

#
# node_GET(request)
#
# Who knows what this does yet?
#

def nodes_GET(request):
	nodeList = model.grid.nodes.values()
	return  JSONResponse({ 'nodes': nodeList }, 200)
