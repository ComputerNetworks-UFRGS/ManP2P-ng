# -*- coding: utf-8 -*-

from twisted.internet.protocol import Protocol, Factory, ClientFactory
from twisted.internet.protocol import  DatagramProtocol as TwistedDatagram
from twisted.internet.address import IPv4Address
from twisted.internet.base import BaseConnector
from twisted.internet.defer import Deferred
from twisted.internet import reactor

from twisted.internet.task import LoopingCall

from time import time
from random import choice

# Command Line:
#	python cyclon.py -n $HOSTNAME -p 143.54.12.86,8001,default,IPv4/UDP \
#	-c 143.54.12.184:8001

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# Very experimental code section
#
#	Important to note that these codes are essential to the implementation
#
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

class BogusMessageError(Exception):
	pass

class DatagramProtocol:
	"""
	ManP2P-ng's 'connected' datagram protocol basic functions. Implementers
	must code only the logic for L{datagramReceived} and L{connectionMade}.
	"""
	peer = None
	port = None
	transport = None

	def getPeer(self):
		return IPv4Address('UDP', self.peer, self.port)

	def getDestination(self):
		return self.getPeer()

	def loseConnection(self):
		"""
		Tell the factory that we are no longer interested is this connection so
		it may be freed at any time.
		"""
		self.factory.disconnect((self.peer, self.port))

	def sendDatagram(self, data):
		self.transport.write(data, (self.peer, self.port))
		self.factory.updateTimerFor((self.peer, self.port))

	# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
	# Abstract Methods
	def connectionMade(self):
		pass

	def datagramReceived(self, data):
		raise NotImplementedError

class DatagramLowerLevel(TwistedDatagram):
	def __init__(self, upper):
		self.upper = upper

	def datagramReceived(self, data, (peer,port)):
		self.upper.dataReceived(data, self, (peer,port))

	def write(self, data, hp):
		self.transport.write(data, hp)

class DatagramFactory:
	"""
	A factory for UDP based protocols.

	As Twisted doesn't have something like this, so we had to implement
	ourselves.
	"""
	loosyState = { }
	listeners = { }
	protocol = None
	keeper = None
	timeout = 10

	def __init__(self, rtr=None):
		self.reactor = rtr if rtr is not None else reactor

	def connectionKeeper(self):
		"""
		Periodically check the connection pool and verifies the last seen time
		so connections don't stay around eternally.
		"""
		l = [ ]

		for k,i in self.loosyState.iteritems():
			if time() - i['lastSeen'] > self.timeout:
				l.append(k)

		for k in l:
			print 'Killing',k
			#self.disconnect(k)

	def startProtocol(self):
		"""
		Starts the connection keeper.
		"""
		self.keeper = LoopingCall(self.connectionKeeper).start(5, False)

	def buildProtocol(self, addr=None):
		p = self.protocol()
		p.factory = self
		return p

	def registerConnectionFor(self, hp, transport):
		"""
		Register the state of the 'connection' for a given peer (peer,port)
		"""
		if hp not in self.loosyState:
			self.loosyState[hp] = { 'protocol': self.buildProtocol(None) }

			self.loosyState[hp]['protocol'].peer = hp[0]
			self.loosyState[hp]['protocol'].port = hp[1]
			self.loosyState[hp]['protocol'].transport = transport

	def getProtocolFor(self, hp):
		"""
		Returns the protocol for a given (host,port)
		"""
		return self.loosyState[hp]['protocol']

	def updateTimerFor(self, hp):
		"""
		Update the last seen timer of a connection. This timer is used to
		decided when a connection shall be dropped for idleness.
		"""
		self.loosyState[hp]['lastSeen'] = time()

	def _startedConnecting(self, connector):
		self.startedConnecting(connector)
		self.reactor.callWhenRunning(connector.connectionMade)

	def connect(self, hp):
		"""
		'Connects' the peer to another peer. In practice, it creates a protocol
		which target to the peer (host,port) as hp and calls the function
		L{connectionMade} of the implementer.
		"""
		self.registerConnectionFor(
			hp, self.listeners[choice(self.listeners.keys())]
		)
		self.updateTimerFor(hp)
		self._startedConnecting(self.getProtocolFor(hp))

	def listen(self, port, interface=None):
		if port in self.listeners:
			raise RuntimeError, 'Port already being listened'

		self.listeners[port] = DatagramLowerLevel(self)
		return self.reactor.listenUDP(port, self.listeners[port], interface)

	def dataReceived(self, data, transport, hp):
		"""
		This functions acts as the 'accepting' function of a TCP flow. It
		register the state of a connection through L{registerConnectionFor} and
		and calls the L{datagramReceived} function of the implementer.
		"""
		self.registerConnectionFor(hp, transport)
		self.updateTimerFor(hp)
		self.getProtocolFor(hp).datagramReceived(data)

	def disconnect(self, hp):
		"""
		Drops the state information about a given peer (host,port) as hp
		"""
		del self.loosyState[hp]

	# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
	# Abstract Methods
	def startedConnecting(self, connector):
		"""
		This function is called when someone calls L{connect} method of this
		class. It shall be used to 'prepare' the ground for the new coming
		connection.
		"""
		pass

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# Helper factories
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

class AbstractWindFactory:
	"""
	Abstract factory for 'winds'
	"""
	defers = { }

	def __init__(self, overlay, rcr=None):
		self.overlay = overlay
		self.reactor = rcr if rcr is not None else reactor

	def connect(self, host, port):
		"""
		Connects to the host given in 'host' and port 'port'.
		"""
		hp, d = (host, port), Deferred()

		if hp not in self.defers:
			self.defers[hp]= [ ]

		self.defers[hp].append(d)
		self.doConnect(hp)

		return d

	def buildProtocol(self, addr):
		p = self.protocol()
		p.overlay = self.overlay

		return p

	def clientConnectionLost(self, connector, reason):
		hp = (connector.getDestination().host,
				connector.getDestination().port)

		if hp in self.defers:
			for d in self.defers[hp]:
				d.errback((reason, connector))

			del self.defers[hp]

	def clientConnectionFailed(self, connector, reason):
		self.clientConnectionLost(connector, reason)

	# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
	# Abstract Methods
	def doConnect(self, (host, port)):
		"""
		This method shall connect the wind to the given host/port.
		"""
		raise NotImplementedError

	def listen(self, port, address=None):
		"""
		Starts to listen in a given port. 'address' may be used as a argument
		for a 'bind' call.
		"""
		raise NotImplementedError

class TCPWindFactory(AbstractWindFactory, Factory, ClientFactory):
	"""
	Concrete factory for TCP-based 'winds'
	"""

	def listen(self, port, address=None):
		interface = '' if address is None else address
		return self.reactor.listenTCP(port, self, interface=interface)

	def doConnect(self, (host, port)):
		return self.reactor.connectTCP(host, port, self)

	def buildProtocol(self, addr):
		p = AbstractWindFactory.buildProtocol(self, addr)
		p.factory = self

		return p

class UDPWindFactory(AbstractWindFactory, DatagramFactory):
	"""
	Concrete factory for UDP-based 'winds'
	"""
	def listen(self, port, address=None):
		interface = '' if address is None else address
		return DatagramFactory.listen(self, port, interface=interface)

	def doConnect(self, (host, port)):
		return DatagramFactory.connect(self, (host, port))

	def buildProtocol(self, addr):
		p = AbstractWindFactory.buildProtocol(self, addr)
		p.factory = self

		return p

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# Custom Exceptions for building the template method
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

class DecodingNotDoneYet(Exception):
	pass

class DecodingFailError(Exception):
	pass

class BogusMessageError(Exception):
	pass

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# Protocol abstractions
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

class AbstractWind:
	"""
	Basic interface for transport layer abstraction classes.
	"""
	overlay = None
	defer = Deferred()

	host = None
	port = None

	addCallback = defer.addCallback
	callback = defer.callback
	errback = defer.errback

	raw = True

	def connectionMade(self):
		hp = (self.getPeer().host, self.getPeer().port)

		if hp in self.factory.defers:
			try:
				d = self.factory.defers[hp].pop(0).callback(self)
			except IndexError:
				del self.factory.defers[hp]

	def preProcess(self, data):
		"""
		Template method for decoding a packet. It will call the interface
		method L{doDecode} to handle protocol or transport specificity.
		"""
		try:
			raw = self.doDecode(data)

		# This exception shall be raised by the interface implementers to
		# signal to the template method that all data to understand the packet
		# is not available yet. For signaling that the packet cannot be
		# decoded, implementers shall rise L{DecodingFailError}
		except DecodingNotDoneYet:
			return

		# This exception shall be raised by interface implementers classes to
		# signal to the template method that this datagram or data stream can
		# not be decoded. The function L{doClean} will be called.
		except DecodingFailError:
			self.doClean(data)

		# Sends the raw data so the Cyclon can inspect and respond to it.
		try:
			self.factory.overlay.processMessage(raw, self)
		except BogusMessageError:
			self.doBogus(data)

	# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
	# Abstract Methods
	def getDescription(self):
		"""
		Shall return a tuple informing the 'network/transport' and the nickname
		of the protocol it accepts (i.e. return 'IPv4/TCP', 'HTTP'). Used for
		advertising peers of how they can contact us.
		"""
		raise NotImplementedError

	@staticmethod
	def getFactoryClass():
		raise NotImplementedError

	def doBogus(self, data):
		"""
		This method is called when a message passed to the overlay can't be
		parsed of handled by a management component.
		"""
		pass

	def doDecode(self, data):
		"""
		This method must remove from data any protocol or transport specificity
		and return it so the Cyclon can parse the message contained in it.

		If the data received so far is not enough (i.e. has not the complete
		message payload), the method shall rise a L{DecodingNotDoneYet}. If the
		data contains bogus payload, it shall rise a L{DecodingFailError}.

		@param data: the message as received by the transport layer.

		@return: the raw Cyclon message.

		@raise DecodingNotDoneYet: if the data received so far is not complete.
		@raise DecodingFailError: if the data is bogus.
		"""
		raise NotImplementedError

	def doSend(self, message, data=None):
		"""
		This method shall send a message to the host to which the transport is
		connected to. It must be responsible for coding the message in the
		format expected by the other side.

		@param message: the message to be sent. Ideally, there is no need for
						verifying it. For "raw" protocols, message is a
						XML object; for non-raw protocols, message is the
						byte stream to be sent

		@param data: raw protocol only field representing the byte stream
						that would be sent if the protocols wasn't raw.
		"""
		raise NotImplementedError

	def doClean(self, data):
		"""
		This method is called when a L{DecodingFailError} is raised by a
		doDecode implementation. It shall be used to cleanup any information
		related to the bogus message.

		Implementers are not obligated to implement this method.
		"""
		pass

	connectionLostAlredyCalled = False

	def connectionLost(self, reason):
		#if self.connectionLostAlredyCalled == False:
			#self.connectionLostAlredyCalled = True
			#self.callback(reason)
		pass

class AbstractTCPWind(AbstractWind, Protocol):
	peer = None
	port = None
	"""
	Interface for TCP based protocols.
	"""
	def connectionMade(self):
		AbstractWind.connectionMade(self)
		Protocol.connectionMade(self)

		_ = self.transport.getPeer()
		_ = (_.host, _.port)

		self.peer, self.port = _

	def dataReceived(self, data):
		self.preProcess(data)

	def getPeer(self):
		return self.transport.getPeer()

	def getHost(self):
		return self.transport.getHost()

class AbstractUDPWind(AbstractWind, DatagramProtocol):
	"""
	Interface for UDP based protocols.
	"""
	def datagramReceived(self, data, hostPort=None):
		self.preProcess(data)

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# Class used externally to archive and get loaded winds
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
class NoSuchAWindError(Exception):
	pass

class WindManager(dict):
	descriptors = { }

	def __setitem__(self, key, value):
		if key not in self.__dict__:
			self.__dict__[key] = value

		dict.__setitem__(self, key, value)

	@staticmethod
	def addDescriptor(descriptor):
		WindManager.descriptors[
						(descriptor.address,descriptor.ports,descriptor.kind)
					] = descriptor

	@staticmethod
	def findWind(kind=None, protocol=None, port=None, host=None):
		l = WindManager.descriptors.values()

		if kind != None:
			l = filter(lambda t: t.kind == kind, l)

		if protocol != None:
			l = filter(lambda t: t.protocol == protocol, l)

		if port != None:
			l = filter(lambda t: port in t.ports, l)

		if host != None:
			l = filter(lambda t: t.host == host, l)

		try:
			return l[0]

		except IndexError:
			raise NoSuchAWindError, "%s %s" % (kind, protocol)

	@staticmethod
	def getDescriptor(host, port, kind):
		"""
		This methods returns the wind descriptor for a wind given the host, port
		and kind of connection. In case that such a wind doesn't exist, it
		raises a L{NoSuchAWindError}.
		"""
		for i in WindManager.descriptors:
			if i[0] == host and i[2] == kind and port in i[1]:
				return WindManager.descriptors[i]

		raise NoSuchAWindError, (host, port, kind)

	@staticmethod
	def getDescriptors():
		return WindManager.descriptors.values()

	@staticmethod
	def load(data):
		errors = [ ]

		for w in data:
			try:
				m = getattr(
					__import__('winds.' + w['module'], fromlist=['*']),
					w['class'])

			except ImportError as e:
				errors.append(
					"Couldn't load wind %s: %s" % (w['class'], str(e)))

			WindManager.addDescriptor(WindDescriptor(w, m))

		if len(errors) > 0:
			raise RuntimeError, str.join('\n', errors)

		# The Doors - The End

	@staticmethod
	def startListening(overlay):
		for w in WindManager.getDescriptors():
			w.listen(overlay)

class WindDescriptor(WindManager):
	"""
	This class holds the winds descriptions as dictionary. Additionally, it
	provides methods so other classes may access a wind based on the host, port
	and kind of connection it server.
	"""
	def __init__(self, dic, wind):
		for k,v in dic.iteritems():
			self[k] = v

		self.wind = wind

	def listen(self, overlayProtocol, rcr=None):
		self.factory = self.wind.getFactoryClass()(overlayProtocol, rcr)
		for p in self.ports:
			self.factory.listen(p, self.address)

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# Non functional code goes here
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

if __name__ == '__main__':
	from twisted.internet import reactor

	class DummyCyclon:
		def messageArrived(self, data, transport):
			print 'Got raw data:', data
			transport.doSend(data)

	class PureUDPProto(AbstractUDPWind):
		def connectionMade(self):
			print '-+- Connection Made -+-'

		def doDecode(self, data):
			return data

		def doSend(self, data):
			self.sendDatagram(data)

	class PureTCPProto(AbstractTCPWind):
		def connectionMade(self):
			print '-+- Connection Made -+-'

		def doDecode(self, data):
			return data

		def doSend(self, data):
			self.transport.write(data)

	overlay = DummyCyclon()

	ufactory = UDPWindFactory(overlay)
	ufactory.protocol = PureUDPProto
	ufactory.listen(8001)

	tfactory = TCPWindFactory(overlay)
	tfactory.protocol = PureTCPProto
	tfactory.listen(8001)

	def derrback(fail):
		print fail

	ufactory.connect('127.0.0.1', 8001)
	tfactory.connect('127.0.0.1', 8001).addErrback(derrback)

	reactor.run()
