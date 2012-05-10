from urllib2 import HTTPError, URLError
from httplib import HTTPException

def request_error(e, err_str):
	if isinstance(e, HTTPError):
		return JSONResponse({ 'error_msg': err_str, 'code': e.code, 'msg': e.msg, 'url': e.url }, http.BAD_REQUEST )
	elif isinstance(e, HTTPException):
		return JSONResponse({ 'error_msg': err_str, 'msg': e.message }, http.BAD_REQUEST )
	elif isinstance(e, URLError):
		return JSONResponse({ 'error_msg': err_str, 'msg': e.reason }, http.BAD_REQUEST )
	else:
		return JSONResponse({ 'error_msg': err_str, 'msg': e }, http.BAD_REQUEST )
