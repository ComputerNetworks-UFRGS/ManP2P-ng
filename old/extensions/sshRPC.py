# -*- coding: utf-8 -*-

from twisted.conch import error
from twisted.conch.ssh import transport, connection
from twisted.conch.ssh import keys, userauth, channel, common
from twisted.internet import defer, protocol, reactor
from twisted.internet.protocol import ClientCreator

from commonRegex import IPHostRegex,HostNameRegex
from extensionLoader import ExtensionLoader
from commandLineParser import CommandLineParser


from random import randint

class ClientCommandTransport(transport.SSHClientTransport):
	def __init__(self, defer, username, password, command):
		self.defer = defer

		self.username = username
		self.password = password
		self.command = command

	def verifyHostKey(self, pubKey, fingerprint):
		# in a real app, you should verify that the fingerprint matches
		# the one you expected to get from this server
		return defer.succeed(True)

	def connectionSecure(self):
		self.requestService(
			PasswordAuth(self.username, self.password,
						ClientConnection(self, self.defer, self.command)))

class PasswordAuth(userauth.SSHUserAuthClient):
	def __init__(self, user, password, connection):
		userauth.SSHUserAuthClient.__init__(self, user, connection)
		self.password = password

	def getPassword(self, prompt=None):
		return defer.succeed(self.password)

class ClientConnection(connection.SSHConnection):
	def __init__(self, clientC, defer, cmd, *args, **kwargs):
		connection.SSHConnection.__init__(self)
		self.clientC = clientC
		self.command = cmd
		self.defer = defer

	def serviceStarted(self):
		self.openChannel(CommandChannel(self.clientC, self.defer,
										self.command, conn=self))

class CommandChannel(channel.SSHChannel):
	name = 'session'

	def __init__(self, clientC, defer, command, *args, **kwargs):
		channel.SSHChannel.__init__(self, *args, **kwargs)
		self.clientC = clientC
		self.command = command
		self.defer = defer

		self.dr = False

		self.allData = ''

	def channelOpen(self, data):
		self.conn.sendRequest(
			self, 'exec', common.NS(self.command), wantReply=True).addCallback(
			self._gotResponse)

	def _gotResponse(self, _):
		self.conn.sendEOF(self)

	def dataReceived(self, data):
		self.allData += data
		self.dr = True

	def closed(self):
		if self.dr:
			self.defer.callback(result=self.allData)
		else:
			self.defer.errback()

		self.clientC.loseConnection()

class ClientCommandFactory(protocol.ClientFactory):
	def __init__(self, username, password, command):
		self.username = username
		self.password = password
		self.command = command

	def buildProtocol(self, addr):
		protocol = ClientCommandTransport(
		self.username, self.password, self.command)

		return protocol

er = 'sshRPC:(({0})|({1}))'.format(
	# An RPC Request
	'request:{rid}@{rip}:{target}:{port}:{user}:{pass}:{command}'.format(**{
		# Information about the requester
		'rid': '[0-9]+',
		'rip': HostNameRegex,
		# Information about the target
		'target': IPHostRegex,
		'port': '[1-9][0-9]*',
		'user': '[a-zA-Z0-9]+',
		'pass': '[a-zA-Z0-9]+',
		'command': '.+',
	}),

	# An RPC Reply
	'reply:{rid}@{rip}:{status}:{message}'.format(**{
		'rid': '[0-9]+',
		'rip': HostNameRegex,
		#
		'status': '((ok)|(nok))',
		'message': '.*',
	}),
)

myself = None

# # # # # # # # # # # # # # # # # #
# For the server
#
amIRPCServer = False

serverMessageQueue = { }

def callCommand(signaler, host, port, user, password, command):
	print "at callCommand"
	print "Running", command, "at", host, port
	d = ClientCreator(reactor, ClientCommandTransport, signaler,
						user, password, command).connectTCP(host, int(port))

	d.addErrback(signaler.errback)

def okReply(result, sParser=None, rid=None):
	global serverMessageQueue

	sParser.write("sshRPC:reply:{0}:ok:{1}".format(rid,result))
	del serverMessageQueue[rid]

def nokReply(fail, sParser, rid):
	global serverMessageQueue

	sParser.write("sshRPC:reply:{0}:nok:".format(rid))
	del serverMessageQueue[rid]

def serverTokenParser(sParser, t):
	global serverMessageQueue

	print "at serverTokenParser"

	args = {'sParser': sParser, 'rid': t[0]}

	d = serverMessageQueue[t[0]] = defer.Deferred()

	reactor.callFromThread(callCommand, d, t[1], t[2], t[3], t[4], ':'.join(list(t[5:])))

	d.addCallback(okReply, **args)
	d.addErrback(nokReply, **args)

# # # # # # # # # # # # # # # # # #
# For the client
#
clientMessageQueue = { }

def sshRPC(where, signaler, host, port, user, password, command):

	global clientMessageQueue

	rid = None
	while rid == None:
		rid = str(randint(0,10000000)) + '@' + myself

		if rid not in clientMessageQueue:
			clientMessageQueue[rid] = signaler
			break

		rid = None

	where.transport.write('sshRPC:request:{0}:{1}:{2}:{3}:{4}:{5}'.format(rid, host, port, user, password, command))

def sshRPCReplyParser(sParser, rid, status, data):
	global clientMessageQueue

	try:
		if status == 'ok':
			clientMessageQueue[rid].callback(data)
		else:
			clientMessageQueue[rid].errback()

		del clientMessageQueue[rid]

	except KeyError as e:
		print "A reply for no question:", e

#
# Generic
#
def sshRPCTparser(sParser, t):
	print "at sshRPCTparser"

	print t[1]
	if t[1] == 'request' and amIRPCServer:
		print "processing a request"
		serverTokenParser(sParser, t[2:])

	elif t[1] == 'reply':
		sshRPCReplyParser(sParser, t[2], t[3], ':'.join(list(t[4:])))

	else:
		pass

# # # # # # # # # # # # #
# Burocratic steps :-) #
# # # # # # # # # # # #
def extensionName():
	return 'sshRPC'

def extendProtocol(lexicalFactory, syntaxFactory):
	global amIRPCServer, myself

	if ExtensionLoader().isActive("Groups"):
		try:
			ExtensionLoader().getExtension("Groups").getGroup("sshRPC")
			amIRPCServer = True
			print "I am server"
		except KeyError as e:
			pass

	syntaxFactory.addToken('sshRPC', sshRPCTparser)

	(lexicalFactory.
			getState('established').
					addTransition('established', er,
									lambda t: t.split(':')))
	(lexicalFactory.
			getState('start').
					addTransition('start', er,
									lambda t: t.split(':')))

	myself = CommandLineParser().getArguments().PeerName

	return True
