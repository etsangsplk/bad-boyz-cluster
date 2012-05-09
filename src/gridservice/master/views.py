import os
import gridservice.utils
from gridservice import http
from gridservice.http import Response, JSONResponse
from gridservice.grid import Job

import gridservice.master.model as model

#
# job_POST
#
# Creates a new Job sent by a client and adds it 
# to the queue
#

def job_POST(request):
	if gridservice.utils.validate_request(request.json, ['executable', 'files']):
		executable = request.json['executable']
		files = request.json['files']

		job = Job(executable, files)
		model.grid.thread.scheduler.add_to_queue(job)

		return JSONResponse({ 'success': "Job added successfully.", 'id': 1 }, 201)
	else:
		return JSONResponse({ 'error_msg': 'Invalid Job JSON received.' }, http.BAD_REQUEST)

#
# job_files_PUT(_GET, _POST, v)
# 
# Takes a binary PUT to a path and stores the file on 
# the local disk based on the id, type and path of the file
#

def job_files_PUT(request, v):
	request.get_raw_to_file(os.path.join("jobs", v['id'], "files", v['type'], v['path']))

	return JSONResponse(v)

#
# node_POST
#
# Takes a ip_address, port and cores and initiates
# the new node in the network.
#

def node_POST(request):
	if gridservice.utils.validate_request(request.get_json(), ['ip_address', 'port', 'cores']): 
		node = request.json

		model.grid.add_node(node)
		return JSONResponse({ 'success': "Node added successfully." }, 201)
	else:
		return JSONResponse({ 'error_msg': 'Invalid Node JSON received.' }, http.BAD_REQUEST)

