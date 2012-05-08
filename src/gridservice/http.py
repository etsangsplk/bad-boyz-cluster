import json
import urllib
import urllib2

OK = "200 OK"
BAD_REQUEST = "400 Bad Request"

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

	def get_response(self, start_response):
		if not self.headers:
			self.add_headers([
				('Content-Type', self.default_content_type),
				('Content-Length', str(len(self.body)))
			])

		start_response(self.status, self.headers)

		return [self.body]

class JSONResponse(Response):

	default_content_type = 'application/json'
	
	def set_body(self, body):
		self.body = json.dumps(body)

class Request(object):

	default_content_type = 'text/plain'

	def __init__(self, url, data):
		self.set_url(url)
		self.set_data(data)
		self.send()

	def set_url(self, url):
		self.url = url

	def set_data(self, data):
		self.data = data

	def set_response_code(self, response_code):
		self.response_code = response_code

	def get_response_code(self):
		return self.response_code

	def set_response(self, response):
		self.response = response

	def get_response(self):
		return self.response;
	
	def send(self):
		request = urllib2.Request(self.url, self.data, {'Content-Type': self.default_content_type})
		response = urllib2.urlopen(request)

		self.set_response_code(response.getcode())
		self.set_response(response.read())

class JSONRequest(Request):

	default_content_type = 'application/json'

	def set_data(self, data):
		self.data = json.dumps(data)

	def send(self):
		super(JSONRequest, self).send()
		self.set_response(json.loads(self.get_response()))
