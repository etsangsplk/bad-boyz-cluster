from urllib2 import HTTPError, URLError
from httplib import HTTPException

from gridservice import http
from gridservice.http import require_json, JSONHTTPRequest, JSONResponse

import gridservice.utils
import gridservice.node.model as model
import gridservice.node.utils as node_utils

from gridservice.node.model import InputFileNotFoundException, ExecutableNotFoundException

@require_json
def task_POST(request):
	d = request.json

	if not gridservice.utils.validate_request(d, 
		['executable', 'flags', 'job_id', 'wall_time']):
		return JSONResponse({ 'error_msg': 'Invalid Job JSON received.' }, http.BAD_REQUEST)

	try:
		task = model.server.add_task(
			executable = d['executable'],
			flags = d['flags'],
			job_id = d['job_id'],
			wall_time = d['wall_time']
		)
	except (InputFileNotFoundException, ExecutableNotFoundException) as e:
		return JSONResponse({ 'error_msg': e.args[0] }, http.BAD_REQUEST)

	return JSONResponse({ 'success': 'Task created.', 'task_id': task.task_id }, 200)

#
# task_files_PUT(request, v)
# 
# Takes a binary PUT to a path and stores the file on 
# the local disk based on the id and path of the file
#

def task_files_PUT(request, v):
	try:
		task = model.server.get_task(v['id'])
	except TaskNotFoundException as e:
		return JSONResponse({ 'error_msg': e.args[0] }, http.NOT_FOUND)

	file_path = task.create_file_path(v['path'])
	request.raw_to_file(file_path)
	task.add_file(file_path)
	
	return JSONResponse(v)

def node_GET(request, v):
	return JSONResponse(v)
