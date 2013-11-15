# -*- coding: utf-8 -*-

from overlay import MessageNotHandledError

class ComponentManager:
	modules = [ ]

	@staticmethod
	def load(data):
		errors = [ ]

		for d in data:
			try:
				m = getattr(
					__import__('components.' + d['name'], fromlist=['*']),
					d['class'])

			except ImportError as e:
				errors.append(
					"Couldn't load module %s: %s" % (d['class'], str(e)))

			else:
				print ' > Loading %s' % (d['name'])
				ComponentManager.modules.append(m)

			if len(errors) > 0:
				for e in errors:
					print e

	@staticmethod
	def register(overlay):
		print '-+- Registering modules -+-'

		for m in ComponentManager.modules:
			print ' > %s registered' % (m.getSubject())
			overlay.addKindHandler(m.getSubject())

class PrivateMethodCallError(Exception):
	pass

class BadMethodCallError(Exception):
	pass

class ManagementComponent:
	def __init__(self, transport=None):
		self.transport = transport

	@staticmethod
	def getSubject():
		raise NotImplementedError

	def handle(self, message):
		theCall = message.getBody().getAttribute("call")

		try:
			assert (theCall[:2] != '__')
			getattr(self, theCall)(message)

		except AssertionError:
			raise PrivateMethodCallError, '''%s called through %s''' % (
											theCall, message.toprettyxml())
		except AttributeError as e:
			raise AttributeError, '''%s called through %s''' % (
									theCall, message.toprettyxml())
		except RuntimeError as e:
			raise e

		except Exception as e:
			raise BadMethodCallError, '''%s called through %s''' % (
										theCall, message.toprettyxml())

class PluginManager:
	plugins = { }

	@staticmethod
	def load(data):
		errors = [ ]

		for d in data:
			try:
				m = getattr(
					__import__('plugins.' + d['name'], fromlist=['*']),
					d['class'])

			except ImportError as e:
				errors.append(
					"Couldn't load plug-in %s: %s" % (d['class'], str(e)))

			else:
				print ' > Loading %s' % (d['name'])
				PluginManager.plugins[d['name']] = m

			if len(errors) > 0:
				for e in errors:
					print e

class PlugIn:
	pass
