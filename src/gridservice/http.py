import os
import json
import urllib
import urllib2
import mimetypes

import BaseHTTPServer

OK = 200
BAD_REQUEST = 400
NOT_FOUND = 404
METHOD_NOT_ALLOWED = 405

class Response(object):
	
	content_type = 'text/plain'

	def __init__(self, body, status=OK, headers = None):
		if headers == None:
			headers = []

		self.body = body
		self.status = status
		self.headers = headers
	
	def add_headers(self, headers):
		self.headers.extend(headers)

	def get_response(self, start_response):
		if not self.headers:
			self.add_headers([
				('Content-Type', self.content_type),
				('Content-Length', str(len(self.body)))
			])

		start_response(self.status_string, self.headers)

		print "Response: " + str(self.status) + ": " + self.body

		return [self.body]

	@property
	def status_string(self):
		responses = BaseHTTPServer.BaseHTTPRequestHandler.responses
		return str(self.status) + " " + responses[self.status][0]

class FileResponse(Response):
	
	def __init__(self, filename, headers = None):

		if headers == None:
			headers = []

		root = "www"
		path = os.path.join(root, filename)
		
		# Check for file injection and existance
		if not os.path.normpath(path).startswith(root) or not os.path.exists(path):	
			status = 404
			body = ""

		# Respond with the file
		else:
			status = 200

			body = file(path, "r").read()
			self.content_type = mimetypes.guess_type(path)[0]
			length = os.path.getsize(path)
			headers.extend([ 
				('Content-Type', self.content_type), 
				('Content-Length', length) 
			])

		super(FileResponse, self).__init__(body, status, headers)

class JSONResponse(Response):

	content_type = 'application/json'
	
	@property
	def body(self):
		return self._body

	@body.setter
	def body(self, body):
		self._body = json.dumps(body)


#
# @require_json decorator
#

def require_json(func):
	def decorator_func(request, *args, **kwargs):
		try:
			request.json
		except ValueError as e:
			return JSONResponse({ 'error': 'Invalid JSON was recieved.' }, 400)

		return func(request, *args, **kwargs)

	return decorator_func


class Request(object):
	
	def __init__(self, env):
		self.env = env
		self.length = env['CONTENT_LENGTH']
		self.content_type = env['CONTENT_TYPE']
		self.query_string = env['QUERY_STRING']

	def _raw(self):
		if self.length:
			return self.env['wsgi.input'].read(int(self.length))
		else:
			return None
		
	def raw_to_file(self, filename):
		fp = open(filename, "w+")
		fp.write(self.raw)
		fp.close()

	@property
	def raw(self):
		if getattr(self, '_raw_cache', None) is None:
			self._raw_cache = self._raw()
		return self._raw_cache

	@property
	def json(self):
		return json.loads( self.raw )
	
	@property
	def post(self):
		return parse_qs( self.raw )

	@property
	def query(self):
		return parse_qs(self.query_string)

class HTTPRequest(object):

	content_type = 'text/plain'

	def __init__(self, method, url, data, headers = None):

		if headers == None:
			headers = {}

		self.url = url
		self.data = data

		self.headers = { 'Content-Type': self.content_type }
		self.headers.update(headers)

		request = urllib2.Request(url, self.data, headers)
		request.get_method = lambda: method
		response = urllib2.urlopen(request)

		self.msg = response.msg
		self.status = response.getcode()
		self.response = response.read()

		print "Request: " + str(self.status) + ": " + str(self.response)

class FileHTTPRequest(HTTPRequest):
	
	def __init__(self, method, url, filename):

		file_data = open(filename, "rb")
		length = os.path.getsize(filename)

		# Files need their content-length specified directly as 
		# you cannot take the length() of a file pointer
		super(FileHTTPRequest, self).__init__(method, url, file_data, { 
			'Content-length': length 
		})

		file_data.close()

class JSONHTTPRequest(HTTPRequest):

	content_type = 'application/json'

	@property
	def response(self):
		return self._response

	@response.setter
	def response(self, response):
		self._response = json.loads(response)

	@property
	def data(self):
		return self._data

	@data.setter
	def data(self, data):
		self._data = json.dumps(data)
