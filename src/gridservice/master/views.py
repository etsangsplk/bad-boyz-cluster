import os
import time
import gridservice.utils
from gridservice import http
from gridservice.http import require_json, Response, FileResponse, JSONResponse
from gridservice.grid import Job, NodeNotFoundException

import gridservice.master.model as model

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
	if gridservice.utils.validate_request(request.json, ['host', 'port', 'cores', 'programs', 'cost']): 
		node = request.json
		node_id = model.grid.add_node(node)
		return JSONResponse({ 'node_id': node_id }, 200)
	else:
		return JSONResponse({ 'error_msg': 'Invalid Node JSON received.' }, http.BAD_REQUEST)

def node_id_GET(request, v):
	node_id = v['id']
	try:
		node = model.grid.get_node(node_id)
	except NodeNotFoundException as e:
		return JSONResponse({ 'error_msg': e.args[0] }, 404)

	return JSONResponse(node, 200)

@require_json
def node_id_POST(request, v):
	if gridservice.utils.validate_request(request.json, ['jobs']): 
		node_id = v['id']

		# Timestamp the heartbeat so we can check its age later
		update = request.json
		update.update({ 'heartbeat_ts': int(time.time()) })
		
		try:
			node = model.grid.update_node(node_id, update)
		except NodeNotFoundException as e:
			return JSONResponse({ 'error_msg': e.args[0] }, 404)

		return JSONResponse(node, 200)
	else:
		return JSONResponse({ 'error_msg': 'Invalid Node JSON received.' }, http.BAD_REQUEST)

#
# index_GET
#
# A nice alias for the console index
#
def index_GET(request):
	return FileResponse("console/console.html")

#
# file_GET
#
# Serves a file directly from disk
#

def file_GET(request, v):
	return FileResponse(v["file"])

#
# node_GET
#
# Who knows what this does yet?
#

def nodes_GET(request):
	nodeList = model.grid.nodes.values()
	return  JSONResponse({ 'nodes': nodeList}, 200)


def jobs_GET(request):
	jobs = model.grid.scheduler.queue

	ljobs=[]
	for j in jobs:
		d = dict()
		d["executable"] = j.executable
		d["name"] = j.name
		d["status"] = j.status
		d["id"] = j.id
		d["work_count_all"] = len(j.work_units)
		d["work_count_queued"] = len( [w for w in j.work_units if w.status=="Queued"])
		d["work_count_active"] = len( [w for w in j.work_units if w.status=="Active"])
		d["work_count_complete"] = len([w for w in j.work_units if w.status=="Complete"])
		ljobs.append(d)

	return  JSONResponse({ 'jobs': ljobs}, 200)


def job_update_POST(request):
	name=None
	cmd=None
	tmp_job_id=-1
	if request.post.has_key("name"):
		name = request.post["name"][0]

	if request.post.has_key("command"):
		cmd = request.post["command"][0]

	if request.post.has_key("tmp_job_id"):
		tmp_job_id = int(request.post["tmp_job_id"][0])

	# Now lets go and create / update the temp job
	if (tmp_job_id == -1):
		tmp_job_id = model.grid.tmp_job_create(cmd, name)
	else:
		tmp_job_id = model.grid.tmp_job_update(tmp_job_id, cmd, name)


	return JSONResponse( {'tmp_job_id': tmp_job_id} , 200)



def job_submit_file_POST(request, v):
	filename = request.query["qqfile"][0]
	tmp_job_id = int(v["tmp_job_id"])


	model.grid.tmp_job_add_file(tmp_job_id, filename, request.raw)


	return JSONResponse( {'tmp_job_id': tmp_job_id, 'filename': filename} , 200)

def job_queue_POST(request):
	if request.post.has_key("tmp_job_id"):
		tmp_job_id = int(request.post["tmp_job_id"][0])

	job_id = model.grid.tmp_job_enqueue(tmp_job_id)

	return JSONResponse( {'job_id': job_id} , 200)
