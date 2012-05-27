from urllib2 import HTTPError, URLError
from httplib import HTTPException

import gridservice.utils
import gridservice.node.model as model
import gridservice.node.utils as node_utils

from gridservice import http
from gridservice.http import require_json, authenticate, JSONResponse
from gridservice.node.model import TaskNotFoundException, InputFileNotFoundException, ExecutableNotFoundException

def auth_server(func):
	def decorator_func(*args, **kwargs):
		return authenticate(model.SERVERS)(func)(*args, **kwargs)
	return decorator_func

@require_json
@auth_server
def task_POST(request):
	d = request.json
	if not gridservice.utils.validate_request(d, 
		['work_unit_id', 'job_id', 'executable', 'filename', 'flags', 'wall_time']):
		return JSONResponse({ 'error_msg': 'Invalid Job JSON received.' }, http.BAD_REQUEST)

	try:
		task = model.server.add_task(
			work_unit_id = d['work_unit_id'],
			job_id = d['job_id'],
			executable = d['executable'],
			filename = d['filename'],
			flags = d['flags'],
			wall_time = d['wall_time']
		)
	except (InputFileNotFoundException, ExecutableNotFoundException) as e:
		return JSONResponse({ 'error_msg': e.args[0] }, http.BAD_REQUEST)

	return JSONResponse({ 'success': 'Task created.', 'task_id': task.task_id }, 200)

@auth_server
def task_id_DELETE(request, v):
	
	try:
		task = model.server.get_task(v['id'])
	except (TaskNotFoundException) as e:
		return JSONResponse({ 'error_msg': e.args[0] }, http.BAD_REQUEST)

	model.server.kill_task(task)

	return JSONResponse({ 'success': 'Task killed.' }, 200)
