#!/usr/bin/env python

from optparse import OptionParser
from paste import httpserver, reloader

import gridservice.utils
import gridservice.node.views as views
import gridservice.node.model as model
from gridservice.http import JSONHTTPRequest, JSONResponse
import time

from urllib2 import HTTPError, URLError
from httplib import HTTPException

from threading import Thread




class NodeServer:
	def __init__(self):
		# Parse the argument from the CLI
		parser = OptionParser()
		
		parser.add_option("-l", "--hostname", dest="host",
			help="The hostname the node should listen on", 
			metavar="HOSTNAME", default = "127.0.0.1")

		parser.add_option("-p", "--port", dest="port",
			help="The port the node should listen on", 
			metavar="PORT", default = 8050)

		parser.add_option("--gh", "--grid_hostname", dest="ghost",
			help="The hostname the node should listen on", 
			metavar="GRID_HOST", default = "127.0.0.1")

		parser.add_option("--gp", "--grid_port", dest="gport",
			help="The port the node should listen on", 
			metavar="GRID_PORT", default = 8051)

		(options, args) = parser.parse_args()

		# Initialise the GridService
		model.grid_service.host = options.ghost
		model.grid_service.port = options.gport

		# Initialise the WSGI Server
		reloader.install()
		
		self.host = options.host
		self.port = options.port

		self.grid_host = options.ghost
		self.grid_port = options.gport

		self.app = gridservice.utils.make_app(routes)

		self.start_heartbeat_async()

		httpserver.serve(self.app, host = self.host, port = self.port)


		
	def start_heartbeat_async(self):
		self.heartbeatThread = Thread(target = self.start_heartbeat)
		self.heartbeatThread.name="Node:Heartbeat"
		self.heartbeatThread.daemon=True
		self.heartbeatThread.start()

	def start_heartbeat(self):# Now lets start sending heartbeats
		while(True):
			self.heartbeat()
			time.sleep(1)

	def heartbeat(self):

		try:
			request = JSONHTTPRequest( 'POST', model.grid_service.url + '/node', 
				{ 
					'ip_address': self.host ,
					'port': self.port,
					'cores': 1,
					'os': 'OSX'
				}
			)
			print "Heartbeat"


		except (HTTPException, URLError) as e:
			print "Heartbeat failed"












routes = {
	('/node/register', 'GET'): views.node_register_GET,
	('/node/{id:\d+}', 'GET'): views.node_GET,
	('/node/{name:\w+}/{id:\d+}', 'GET'): views.node_GET
}

if __name__ == '__main__':

	server = NodeServer()

	# # Parse the argument from the CLI
	# parser = OptionParser()
	
	# parser.add_option("-l", "--hostname", dest="host",
	# 	help="The hostname the node should listen on", 
	# 	metavar="HOSTNAME", default = "127.0.0.1")

	# parser.add_option("-p", "--port", dest="port",
	# 	help="The port the node should listen on", 
	# 	metavar="PORT", default = 8050)

	# parser.add_option("--gh", "--grid_hostname", dest="ghost",
	# 	help="The hostname the node should listen on", 
	# 	metavar="HOSTNAME", default = "127.0.0.1")

	# parser.add_option("--gp", "--grid_port", dest="gport",
	# 	help="The port the node should listen on", 
	# 	metavar="PORT", default = 8051)

	# (options, args) = parser.parse_args()

	# # Initialise the GridService
	# model.grid_service.host = options.ghost
	# model.grid_service.port = options.gport

	# # Initialise the WSGI Server
	# reloader.install()
	
	# host = options.host
	# port = options.port

	# app = gridservice.utils.make_app(routes)

	# start_server_async()

