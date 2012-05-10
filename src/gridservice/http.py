import os
import json
import urllib
import urllib2

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
		self.status= status
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

		self.data = data
		self.url = url
		self.method = method
		self.failure = False
		self.headers = headers

		self.send()

	@property
	def has_failed(self):
		return bool(self.failure)

	@property
	def status_string(self):
		responses = BaseHTTPServer.BaseHTTPRequestHandler.responses
		return str(self.status) + " " + responses[self.status][0]

	@property
	def headers(self):
		headers = dict(self._headers)
		headers.update({ 'Content-Type': self.content_type })
		return headers

	@headers.setter
	def headers(self, headers):
		self._headers = headers

	def add_headers(self, headers):
		self._headers.update(headers)
	
	def send(self):
		request = urllib2.Request(self.url, self.data, self.headers)
		request.get_method = lambda: self.method
		
		try:
			response = urllib2.urlopen(request)

		except urllib2.HTTPError as e:
			self.status = e.code
			self.response = e.read()
			self.failure = self.status_string

		except urllib2.URLError as e:
			self.failure = e.reason[1]

		else:
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
		try:
			return json.loads(self._response)
		except TypeError as e:
			raise InvalidResponseJSONException(self._response + " is invalid JSON")

	@response.setter
	def response(self, response):
		self._response = response

	@property
	def data(self):
		return self._data

	@data.setter
	def data(self, data):
		try:
			self._data = None
		except TypeError as e:
			raise InvalidRequestJSONException(data + " is invalid JSON")

class InvalidResponseJSONException(Exception):
	pass

class InvalidRequestJSONException(Exception):
	pass
