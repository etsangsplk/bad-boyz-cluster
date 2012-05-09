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

def job_POST(_GET, _POST):
	if gridservice.utils.validate_request(_POST, ['executable', 'files']):
		executable = _POST['executable']
		files = _POST['files']

		job = Job(executable, files)
		model.grid.scheduler.add_to_queue(job)

		return JSONResponse({ 'success': "Job added successfully." }, 201)
	else:
		return JSONResponse({ 'error_msg': 'Invalid Job JSON received.' }, http.BAD_REQUEST)

#
# node_POST
#
# Takes a ip_address, port and cores and initiates
# the new node in the network.
#

def node_POST(_GET, _POST):
	if gridservice.utils.validate_request(_POST, ['ip_address', 'port', 'cores']): 
		node = _POST
		model.grid.add_node(node)
		return JSONResponse({ 'success': "Node added successfully." }, 201)
	else:
		return JSONResponse({ 'error_msg': 'Invalid Node JSON received.' }, http.BAD_REQUEST)

