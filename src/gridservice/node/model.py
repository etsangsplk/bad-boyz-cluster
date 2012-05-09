class GridService:
	
	def __init__(self):
		self.host = ""
		self.port = ""

	@property
	def url(self):
		return self._url()

	def _url(self):
		return "http://%s:%s" % (self.host, int(self.port))

	def __str__(self):
		return self.url

grid_service = GridService()
