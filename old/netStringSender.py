# -*- coding: utf-8 -*-
from twisted.internet.protocol import ClientFactory
from twisted.protocols.basic import NetstringReceiver

from twisted.internet import reactor

from sys import argv,exit

PUT = '''http-dtn:PUT * HTTP/1.1
Host: alderan
Content-Destination: polismassa
Content-Source: alderan
Content-Length: 3
Content-Range: bytes 0/3
Content-MD5: e07910a06a086c83ba41827aa00b26ed
Date: Fri, 13 Feb 2009 23:31:30 GMT

asd'''

GET = '''http-dtn:GET * HTTP/1.1
Host: alderan
Data: essa data

'''
ask = PUT 

class NetStringSender(NetstringReceiver):
	def connectionMade(self):
		print "-+- Connection Made -+-"

		self.sendString(ask)
		reactor.callInThread(self.dataToSend)

	def stringReceived(self, data):
		print "\n-+- Message Received -+-"
		print data + '\n'

		reactor.callInThread(self.dataToSend)

	def dataToSend(self):
		try:
			msg = raw_input("Message: ")

		except EOFError:
			self.factory.shallRetry = False
			self.transport.loseConnection()

			return

		if msg == '^[{}]^':
			self.factory.shallRetry = False
			self.transport.loseConnection()
			return

		self.sendString(msg)

class ThisFactory(ClientFactory):
	protocol = NetStringSender

	def __init__(self):
		self.shallRetry = True

	def clientConnectionFailed(self, connector, reason):
		if self.shallRetry:
			print "Connection failed. Retrying in 3 seconds\n"
			reactor.callLater(3, connector.connect)

	def clientConnectionLost(self, connector, reason):
		if self.shallRetry:
			print "Connection was lost. Retrying in 3 seconds\n"
			reactor.callLater(3, connector.connect)

		else:
			reactor.stop()

reactor.connectTCP(argv[1], int(argv[2]), ThisFactory())
reactor.run()
