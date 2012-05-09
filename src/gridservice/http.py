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
	
	default_content_type = 'text/plain'

	def __init__(self, body, status=OK, headers = []):
		self.set_body(body)
		self.set_status(status)
		self.set_headers(headers)
	
	def set_status(self, status):
		self.status = status
	
	def set_headers(self, headers):
		self.headers = headers

	def add_headers(self, headers):
		self.headers.extend(headers)

	def set_body(self, body):
		self.body = body

	def get_status_string(self):
		responses = BaseHTTPServer.BaseHTTPRequestHandler.responses
		return str(self.status) + " " + responses[self.status][0]

	def get_response(self, start_response):
		if not self.headers:
			self.add_headers([
				('Content-Type', self.default_content_type),
				('Content-Length', str(len(self.body)))
			])

		start_response(self.get_status_string(), self.headers)

		return [self.body]

class JSONResponse(Response):

	default_content_type = 'application/json'
	
	def set_body(self, body):
		self.body = json.dumps(body)

class Request(object):

	default_content_type = 'text/plain'

	def __init__(self, method, url, data, headers={}):
		self.set_url(url)
		self.set_data(data)
		self.set_method(method)
		self.failure = False
		self.set_response("")
		self.headers = headers

		self.send()

	def set_url(self, url):
		self.url = url

	def set_data(self, data):
		self.data = data

	def set_method(self, method):
		self.method = method

	def set_status(self, status):
		self.status = status

	def get_status(self):
		return self.status

	def set_response(self, response):
		self.response = response

	def get_response(self):
		return self.response;

	def get_status_string(self):
		responses = BaseHTTPServer.BaseHTTPRequestHandler.responses
		return str(self.status) + " " + responses[self.status][0]

	def get_headers(self):
		self.headers.update({ 'Content-Type': self.default_content_type })
		return self.headers
	
	def failed(self):
		return self.failure

	def send(self):
		request = urllib2.Request(self.url, self.data, self.get_headers())
		request.get_method = lambda: self.method
		
		try:
			response = urllib2.urlopen(request)

		except urllib2.HTTPError as e:
			self.set_status(e.code)
			self.set_response(e.read())
			self.failure = self.get_status_string()

		except urllib2.URLError as e:
			self.failure = e.reason[1]

		else:
			self.set_status(response.getcode())
			self.set_response(response.read())

class FileRequest(Request):
	
	def __init__(self, method, url, filename):
		file_data = open(filename, "rb")
		length = os.path.getsize(filename)

		super(FileRequest, self).__init__(method, url, file_data, { 
			'Content-length': length 
		})

class JSONRequest(Request):

	default_content_type = 'application/json'

	def set_data(self, data):
		self.data = json.dumps(data)

	def send(self):
		super(JSONRequest, self).send()
		if self.get_response():
			self.set_response(json.loads(self.get_response()))

