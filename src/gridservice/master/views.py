import os
import gridservice.utils
from gridservice import http
from gridservice.http import require_json, Response, JSONResponse
from gridservice.grid import Job

import gridservice.master.model as model

import io
#
# job_POST
#
# Creates a new Job sent by a client and adds it 
# to the queue
#

@require_json
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
	request.raw_to_file(os.path.join("jobs", v['id'], "files", v['type'], v['path']))

	return JSONResponse(v)

#
# node_POST
#
# Takes a ip_address, port and cores and initiates
# the new node in the network.
#

@require_json
def node_POST(request):
	if gridservice.utils.validate_request(request.json, ['ip_address', 'port', 'cores', 'current_job', 'cpu']): 
		node = request.json

		model.grid.add_node(node)
		return JSONResponse({ 'success': "Node added successfully." }, 201)
	else:
		return JSONResponse({ 'error_msg': 'Invalid Node JSON received.' }, http.BAD_REQUEST)



###############################################
####### 	This is the Console yo!  ##########
###############################################


# Basic handlers for the consoles files
def index_GET(request):
	return Response(status=200, headers=[('content-type', 'text/html')], body=file("console/console.html", "r").read())

def css_GET(request, v):
	# Get whatever CSS has been requested...
	return Response(status=200, headers=[('content-type', 'text/css')], body=file("console/css/" + v["file"] + ".css", "r").read())

def js_GET(request, v):
	# Get whatever CSS has been requested...
	return Response(status=200, headers=[('content-type', 'text/javascript')], body=file("console/js/" + v["file"] + ".js", "r").read())

def png_GET(request, v):
	# Get whatever CSS has been requested...
	return Response(status=200, headers=[('content-type', 'image/png')], body=file("console/img/" + v["file"] + ".png", "r").read())

def jpg_GET(request, v):
	# Get whatever CSS has been requested...
	return Response(status=200, headers=[('content-type', 'image/jpeg')], body=file("console/img/" + v["file"] + ".jpg", "r").read())

# Now some handlers for the JSON action

def nodes_GET(request):
	nodeList = model.grid.nodes.values()

	return  JSONResponse({ 'success': "Job added successfully.", 'nodes': nodeList }, 200)

