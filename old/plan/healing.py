# -*- coding: utf-8 -*-

from twisted.conch import error
from twisted.conch.ssh import transport, connection
from twisted.conch.ssh import keys, userauth, channel, common
from twisted.internet import defer, protocol, reactor
from twisted.internet.protocol import ClientCreator

class ClientCommandTransport(transport.SSHClientTransport):
	def __init__(self, defer, username, password, kind, command):
		self.defer = defer

		self.username = username
		self.password = password
		self.command = command

		self.kind = kind

	def verifyHostKey(self, pubKey, fingerprint):
		# in a real app, you should verify that the fingerprint matches
		# the one you expected to get from this server
		return defer.succeed(True)

	def connectionSecure(self):
		self.requestService(
			PasswordAuth(self.username, self.password,
						ClientConnection(self, self.defer,
										 self.kind, self.command)))

class PasswordAuth(userauth.SSHUserAuthClient):
	def __init__(self, user, password, connection):
		userauth.SSHUserAuthClient.__init__(self, user, connection)
		self.password = password

	def getPassword(self, prompt=None):
		return defer.succeed(self.password)

class ClientConnection(connection.SSHConnection):
	def __init__(self, clientC, defer, kind, cmd, *args, **kwargs):
		connection.SSHConnection.__init__(self)
		self.clientC = clientC
		self.command = cmd
		self.kind = kind
		self.defer = defer

	def serviceStarted(self):
		self.openChannel(CommandChannel(self.clientC, self.defer, self.kind,
										self.command, conn=self))

class CommandChannel(channel.SSHChannel):
	name = 'session'

	def __init__(self, clientC, defer, kind, command, *args, **kwargs):
		channel.SSHChannel.__init__(self, *args, **kwargs)
		self.clientC = clientC
		self.command = command
		self.kind = kind
		self.defer = defer

		self.dr = False

	def channelOpen(self, data):
		self.conn.sendRequest(
			self, 'exec', common.NS(self.command), wantReply=True).addCallback(
			self._gotResponse)

	def _gotResponse(self, _):
		self.conn.sendEOF(self)

	def dataReceived(self, data):
		if data.split()[-1] == 'Completed.':
			print "Done"
			self.defer.callback(True)
			self.dr = True

	def closed(self):
		if not self.dr:
			self.defer.errback(False)

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

def execute(host, port, df):
	d = ClientCreator(reactor, ClientCommandTransport, df,
					'root', '192168061', 'server',
					'/etc/init.d/ossec restart',
	).connectTCP(host, port)

	d.addErrback(df.errback, **{'how': False})

if __name__ == '__main__':
	d = defer.Deferred()

	execute('143.54.12.74', 22, d)
	reactor.run()
