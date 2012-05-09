#!/usr/bin/env python

from gridservice.http import JSONRequest, JSONResponse

url = "http://localhost:8051"

request = JSONRequest( 'POST', url + '/job', 
	{ 
		'executable': 'localhost',
		'files': ['file1.txt', 'file2.txt', 'file3.txt']
	}
)

if not request.failed():
	print request.get_response()
else:
	print request.failed()
