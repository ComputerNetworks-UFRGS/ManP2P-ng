
from extension import ManagementComponent

class DummyModule(ManagementComponent):

	@staticmethod
	def getSubject():
		return 'TheDummyModule'
