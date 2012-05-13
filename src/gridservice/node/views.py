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
		['executable', 'flags', 'work_unit_id', 'job_id', 'wall_time']):
		return JSONResponse({ 'error_msg': 'Invalid Job JSON received.' }, http.BAD_REQUEST)

	try:
		task = model.server.add_task(
			executable = d['executable'],
			filename = d['filename'],
			flags = d['flags'],
			job_id = d['job_id'],
			work_unit_id = d['work_unit_id'],
			wall_time = d['wall_time']
		)
	except (InputFileNotFoundException, ExecutableNotFoundException) as e:
		return JSONResponse({ 'error_msg': e.args[0] }, http.BAD_REQUEST)

	return JSONResponse({ 'success': 'Task created.', 'task_id': task.task_id }, 200)

def node_GET(request, v):
	return JSONResponse(v)
