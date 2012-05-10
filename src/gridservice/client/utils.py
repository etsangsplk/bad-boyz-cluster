from urllib2 import HTTPError, URLError
from httplib import HTTPException

def request_error(e, err_str):
	if isinstance(e, HTTPError):
		print "%s\nRequest to: %s\nResponse: %s %s" % (err_str, e.url, e.code, e.msg)
	elif isinstance(e, HTTPException):
		print "%s\nMessage: %s" % (err_str, e.message)
	elif isinstance(e, URLError):
		print "%s\nMessage: %s" % (err_str, e.reason)
	else:
		print "An unknown error occured with your request: %r" % e
