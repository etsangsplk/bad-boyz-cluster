#!/usr/bin/env python

from paste import httpserver, reloader

import gridservice.utils
import gridservice.master.views as views

routes = {
	('/node', 'POST'): views.node_POST,
	('/job', 'POST'): views.job_POST
}

if __name__ == '__main__':

	# Initalise the WSGI Server
	reloader.install()

	host = '127.0.0.1'
	port = 8051

	app = gridservice.utils.make_app(routes)
	httpserver.serve(app, host = host, port = port)
