# -*- coding: utf-8 -*-

class SyntaxParserFactory:
	def __init__(self):
		self.tokens = { }

	def addToken(self, token, function):
		if token in self.tokens:
			raise TokenAlreadyInSetError

		self.tokens[token] = function

	def getAParser(self, connection, transport, factory):
		return SyntaxParser(self.tokens, connection, transport, factory)

class SyntaxParser:
	def __init__(self, tokens, connection, transport, factory):
		self.tokens = tokens

		self.transport = transport
		self.factory = factory

		# Peer List
		self.plist = self.factory.peerlist

		# Peer name
		self.myNick = self.factory.myNick

		# Just some wrappers.
		#	Let's write a little bit less code
		#
		self.write = connection.sendString
		self.disconnect = transport.loseConnection

		self.remotePeer = None
		self.remotePeerNick = None
		self.remotePeerPort = None

	def parseData(self, tokens):
		try:
			self.tokens[tokens[0]](self, tokens)
		except KeyError:
			raise SyntaxError, "Token not reconized " + str(tokens)
