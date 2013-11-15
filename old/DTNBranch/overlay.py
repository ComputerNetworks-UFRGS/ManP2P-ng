# -*- coding: utf-8 -*-

from twisted.internet.protocol import Protocol
from twisted.internet.protocol import Factory
from twisted.internet.protocol import ClientFactory
from twisted.protocols.basic import NetstringReceiver

from twisted.internet import reactor

from twisted.python import threadable

from peerlist import PeerList
from commandLineParser import CommandLineParser
from extensionLoader import ExtensionLoader

from protocolLexicalParser import LexicalParserFactory
from protocolSyntaxParser import SyntaxParserFactory

from sys import stderr

errwrite = stderr.write

EXTENSIONDIR = 'extensions-enabled'
LPORT = 8001
verboseIsOn = True

bootStrapFunctions = [ ]
haltingFunctions = [ ]

clientBootStrapFunctions = [ ]

def addFunctionInList(l, f, push):
	if push:
		l.insert(0, f)
	else:
		l.append(f)

def addBootstrapFunction(bsfunction, push=False):
	addFunctionInList(bootStrapFunctions, bsfunction, push)

def addHaltFunction(hfunction, push=False):
	addFunctionInList(haltingFunctions, hfunction, push)

def addClientBootstrapFunction(bsfunction, push=False):
	addFunctionInList(clientBootStrapFunctions, bsfunction, push)

def listenPort():
	return LPORT

class OverlayListener(NetstringReceiver):
	def __init__(self):
		# Protocol parsers
		#	Lexical parser
		self.lParser = None
		#	Syntax parser
		self.sParser = None

	def connectionMade(self):
		self.lParser = self.factory.lParserFactory.getAParser()
		self.sParser = self.factory.sParserFactory.getAParser(self,
															  self.transport,
															  self.factory)

		for f in bootStrapFunctions:
			f(self)

	def stringReceived(self, data):
		if verboseIsOn:
			print "-+- Data received -+-\n" + data

		try:
			t = self.lParser.parseData(data)

			if not t:
				return
		except SyntaxError:
			self.sendString(data.split(':')[0] + ':e:unknownCommand')
			print "WARNING: No one parsed this sequence\t"
			return

		if verboseIsOn:
			print "Parsed data:"
			print t

		self.sParser.parseData(t)
		self.sParser.transport.doWrite()
		print ''

	def connectionLost(self, reason):
		for f in haltingFunctions:
			f(self)

		self.transport.loseConnection()

class OverlayConnector(OverlayListener):
	def __init__(self):
		OverlayListener.__init__(self)

	def connectionMade(self):
		OverlayListener.connectionMade(self)

		for f in clientBootStrapFunctions:
			f(self)

class OverlayListenerFactory(Factory):
	protocol = OverlayListener
	__extended = False

	def __init__(self, overlay):
		self.overlay = overlay

		self.peerlist = overlay.peerlist
		self.myNick = overlay.myNick

		self.lParserFactory = overlay.lexicalFactory
		self.sParserFactory = overlay.syntaxFactory

class OverlayConnectorFactory(ClientFactory, OverlayListenerFactory):
	protocol = OverlayConnector

	def __init__(self, overlay):
		OverlayListenerFactory.__init__(self, overlay)

	def clientConnectionFailed(self, connector, reason):
		errwrite("Conection to " + connector.getDestination().host +
				 " has failed.\n" + reason.getErrorMessage() + '\n')

class Overlay:
	def __init__(self):
		self.lexicalFactory = None
		self.syntaxFactory = None

		self.listenerFactory = None
		self.connectorFactory = None

		self.myNick = None

	def setupProtocol(self):
		self.lexicalFactory = LexicalParserFactory('start')
		self.syntaxFactory = SyntaxParserFactory()

		(ExtensionLoader(self.lexicalFactory, self.syntaxFactory).
												loadExtensions(EXTENSIONDIR))

	def startOverlay(self):
		self.peerlist = PeerList()

		CommandLineParser().parse_args()
		self.myNick = CommandLineParser().getArguments().PeerName

		ExtensionLoader().extendProtocol()

		self.listenerFactory = OverlayListenerFactory(self)
		self.connectorFactory = OverlayConnectorFactory(self)

		for port in CommandLineParser().getArguments().lport:
			reactor.listenTCP(port, self.listenerFactory)

		for t in CommandLineParser().getArguments().mhost:
			a = t.split(':')
			peer, port = a[0], int(a[1]) if len(a) > 1 else LPORT

			reactor.connectTCP(peer, port, self.connectorFactory)

			del a

		threadable.init()
		reactor.run()
