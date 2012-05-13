import time
import sys
from threading import Thread

import gridservice.node.monitor as monitor
import gridservice.node.utils as node_utils

from gridservice.http import JSONHTTPRequest, JSONResponse

from urllib2 import HTTPError, URLError
from httplib import HTTPException

class NodeServer:
	def __init__(self, host, port, ghost, gport):

		self.host = host
		self.port = port
		self.ghost = ghost
		self.gport = gport
		self.jobs = []

		self.cores = 1
		self.os = 'OSX'
		self.programs = [ 'adder' ]
		self.cost = 15

		# THIS LINE IS FOR DEBUGGING, DELAYS REGISTER TO GIVE
		# TIME FOR THE SERVER TO REBOOT ON CODE SAVE
		time.sleep(2)

		self.node_id = self.register_node()

		# Start the Monitor
		self.mon = monitor.Monitor()

		# Start the Heartbeat
		self.heartbeat_interval = 5
		self.start_heartbeat()

	@property
	def grid_url(self):
		return "http://%s:%s" % (self.ghost, self.gport)

	def start_heartbeat(self):
		self.heartbeatThread = Thread(target = self.heartbeat)
		self.heartbeatThread.name = "Node:Heartbeat"
		self.heartbeatThread.daemon = True
		self.heartbeatThread.start()

	def heartbeat(self):
		while True:
			self.send_heartbeat()
			time.sleep(self.heartbeat_interval)

	def send_heartbeat(self):
		try:
			request = JSONHTTPRequest( 'POST', self.grid_url + '/node/' + str(self.node_id), { 
				'cpu': self.mon.cpu(),
				'jobs': self.jobs,
			})
			print "Heartbeat: (CPU: " + str(self.mon.cpu()) + "%)"

		except (HTTPException, URLError) as e:
			node_utils.request_error_cli(e, "Heatbeat Failed: Unable to establish a connection to the grid")

	
	def register_node(self):
		try:
			request = JSONHTTPRequest( 'POST', self.grid_url + '/node', { 
				'host': self.host,
				'port': self.port,
				'cores': self.cores,
				'os': self.os,
				'programs': self.programs,
				'cost': self.cost,
			})
		except (HTTPException, URLError) as e:
			node_utils.request_error_cli(e, "Unable to establish a connection to the grid")
			sys.exit(1)
	
		return request.response['node_id']
