#!/usr/bin/env python

import os
from gridservice.http import FileHTTPRequest, JSONHTTPRequest, JSONResponse

url = "http://localhost:8051"

executable = 'myexec'
files = [ 'file1.txt' ]

request = JSONHTTPRequest( 'POST', url + '/job', { 
	'executable': executable,
	'files': files
})

if not request.failed():
	res = request.get_response()
	
	for filename in files:
		req_path = url + '/job/' + str(res['id']) + '/files/executable/' + filename
	
		request = FileHTTPRequest( 'PUT', req_path, filename )
		
		if not request.failed():
			print request.get_response()
		else:
			print req_path
			print request.failed()

else:
	print request.failed()
