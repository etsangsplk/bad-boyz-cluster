
import urllib
import SimpleHTTPServer
from BaseHTTPServer import BaseHTTPRequestHandler, HTTPServer

class WorkerHandler(BaseHTTPRequestHandler):

   # def __init__(self):
        #self.DB = database
        #print "Handler __init__"
    def bind_server(self, node):
        self.worker = node

    def do_POST(self):
        parts = self.path.split("/")
        

        if parts[1] == "Upload":
                jobId = parts[2]
                filename = parts[3]

                length = int(self.headers.getheader('content-length'))
                postData = self.rfile.read(length)
                    
                self.upload_file(jobId, filename, postData)
        if parts[1] == "Execute":
                jobId = parts[2]

                length = int(self.headers.getheader('content-length'))
                postData = self.rfile.read(length)
                    
                self.execute(jobId, postData)


    def do_GET(self):
        parts = self.path.split("/")

        if parts[1] == "Status":
                jobId = parts[2]
                self.get_status(jobId)

        if parts[1] == "Execute":
                jobId = parts[2]
                self.get_status(jobId)
                

    def upload_file(self, jobId, filename, data):
        # Write the file to a temp folder named "jobIb"
        pass

    def get_status(self, jobId):
        pass

    def execute(self, jobId, command):
        pass


class WorkerNode():
    def __init__(self, port, head_address):
        self.head_address = head_address
        self.port = int(port)
        self.httpDaemon = WorkerHandler(("", self.port), CommandHandler)
        self.httpDaemon.bind_server(self)

