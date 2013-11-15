# -*- coding: utf-8 -*-

from message import Message, ExpatError, MessageKeeper
from wind import WindManager, NoSuchAWindError

from sys import stderr

class HandlerAlreadyInSetError(Exception):
	pass

class MessageNotHandledError(Exception):
	pass

class KeepProcessing(Exception):
	pass

class AbstractOverlay:
	peerList = None
	myself = None

	kindHandlers = { }

	def start(self, properties, winds):
		raise NotImplementedError

	def bootstrap(self, fail=None, peers=[]):
		'''
		Bootstraps the peer into the (P2P) network. It calls the specific
		method 'doBootstrap' to handle overlay specific steps.
		'''
		node = None

		try:
			node = peers.pop(0)
		except IndexError:
			print ' *\033[91m Bootstrapping failed:\033[0m cannot reach any introducer node.'
			return

		if fail is None:
			print '-+- Bootstrapping into the network -+-'
		else:
			print >> stderr, '   * Bootstrapping with this peer has failed'

		print ' | trying with %s, port %s, kind %s and protocol %s' % (
			node['address'], node['port'], node['kind'], node['protocol']
		)

		try:
			wind = WindManager.findWind(
				kind=node['kind'], protocol=node['protocol'])
		except NoSuchAWindError:
			return self.bootstrap(peers=peers)

		try:
			d = wind.factory.connect(node['address'], node['port'])

			d.addCallback(self.doBootstrap, peers=peers)
			d.addErrback(self.bootstrap, peers=peers)

		except AttributeError as e:
			print ' * Bootstrapping with %s (%s:%s) protocol %s has failed' % (
						node['address'], node['port'],
						node['kind'], node['protocol']
			)
			print '   Can\'t find a suitable wind for this bootstrap node.'
			return

	def messageArrived(self, transport, message):
		'''
		This method shall be implemented by the overlay code to treat
		overlay specific messages (like the "exchange" in Cyclon).
		'''
		raise NotImplementedError

	def processMessage(self, raw, transport):
		'''
		A template method for processing messages arriving at winds.

		@param raw: the raw message which arrived
		@param transport: the wind on which the message arrived
		'''

		# First of all, we parse the message to check if it contains valid XML
		# data, which is the kind of thing we are expecting
		try:
			message = Message(data=raw)

		# Is not XML, so let's say it loud and quit processing it.
		except ExpatError as e:
			cantParseIt = '-+- Can\'t parse the following raw data -+-'
			print >> stderr, (cantParseIt + '\n' + raw + '\n')
			raise BogusMessageError

		# Protocol debug data
		# Just to show the message content to the users. Nothing more.
		print '-+- Message Received -+-\n', message.toprettyxml()

		# If the message is a reply to another earlier sent message, we avoid
		# the overlay code knowing about it and send it directly to the reply
		# handler set during the message sending.
		if (message.getKind() == 'reply'):
			MessageKeeper.callback((transport, message))

		# However, if the message is not a reply, we first check if the overlay
		# wish to consume it and, if so, we don't send it to the handlers.
		elif self.tryToInfer(transport, message):
			# Ok, if we get to this point the overlay know nothing about the
			# kind of the message. So, it is probably a plugin/management
			# component related message and we must send it to its handler.
			try:
				self.handle(transport, message)

			# If the handler was not found, we show the following error
			# message so everyone notices it!
			except MessageNotHandledError:
				print >> stderr, '\n * This message was not handled\n'

		# Protocol debug data
		# List the peers we are directly connected to.
		print '-+- Ours peers -+-'

		for i in self.peerList:
			print ' *', i

	def tryToInfer(self, transport, message):
		mkind = str(message.getKind()).replace(' ', '')

		try:
			assert(mkind[:2] != '__')
			getattr(self, mkind + 'Received')((transport, message))

		except KeepProcessing as e:
			return True

		return False

	def addKindHandler(self, kind, handler=None):
		if kind in self.kindHandlers:
			raise HandlerAlreadyInSetError, kind

		self.kindHandlers[kind] = handler

	def handle(self, kind, message):
		try:
			managementComponent = self.kindHandlers[str(message.getSubject())]
		except (KeyError, AttributeError):
			raise MessageNotHandledError

		managementComponent(transport).handle(message)

	def getDescription(self):
		raise NotImplementedError

	def broadcast(self, message):
		raise NotImplementedError


class OverlayManager:
	__theOverlay = None

	@staticmethod
	def getOverlay():
		return OverlayManager.__theOverlay()

	@staticmethod
	def load(data):
		try:
			o = getattr(
				__import__('overlays.' + data['module'], fromlist=['*']),
				data['class'])
		except ImportError as e:
			raise RuntimeError, (
				"Couldn't load overlay %s: %s" % (data['class'], str(e)))

		OverlayManager.__theOverlay = o
