# -*- coding: utf-8 -*-
#                                                                           80

from time import time

verboseIsOn = 1

def isIP(ip):
	try:
		octets = map(int, ip.split('.'))
	except ValueError:
		return False

	if len(octets) < 4:
		return False

	for i in octets:
		if i < 0 or i >= 255:
			return False

	return True

class PeerAlreadyInListError(Exception):
	def __init__(self, peer, nickname):
		self.peer = peer
		self.nick = nickname

	def __str__(self):
		return self.nickname + " (" + self.peer + ")"

class NoSuchAPeerError(Exception):
	def __init__(self, peer):
		self.peer = peer

	def __str__(self):
		return self.peer

class Peer:
	def __init__(self, peer, nick, parser):
		self.peer = peer.host
		self.port = peer.port
		self.nick = nick

		self.transport = parser

		self.anotations = { }

	def setAnotation(self, attribute, value):
		self.anotations[attribute] = value

	def getAnotation(self, attribute):
		return self.anotations[attribute]

	# Format the output string:
	#	%n >> Peer nick
	#	%p >> Peer listening ports
	#	%P >> Peer IP address
	#	%% >> Percent
	#
	def setFormat(format):
		token = False
		self.formated = ''

		for c in format:
			if c == '%' and not token:
				token == True

			elif token:
				if c == 'p':
					self.formated += self.peer
				if c == 'n':
					self.formated += self.nick
				if c == '%':
					self.formated += '%'

			elif not Token:
				self.formated += c

			else:
				raise SyntaxError, "Can't parse format"

	def __str__(self):
		if self.formated:
			return self.formated

		return "{0},{1}".format(self.nick, self.host)

	def __eq__(self, other):
		if type(other) == str:
			return True if other == self.nick else False

	def __del__(self):
		del self.anotations

class PeerList:
	def __init__(self):
		self.addrs = { }

	def addPeer(self, peer, nick, sParser):
		if nick in self.addrs:
			raise PeerAlreadyInListError(peer, nick)

		t = Peer(peer, nick, sParser)

		self.addrs[nick] = t

		if verboseIsOn:
			print "Adding peer nicknamed " + nick

		return t

	def removePeer(self, peer):
		try:
			t = self.getPeer(peer)
		except NoSuchAPeerError:
			return

		if verboseIsOn:
			print	"Peer " + t.nick + " (" + t.peer + \
					") is leaving the overlay"

		del self.addrs[t.nick]
		del t

	def getPeer(self, peer):
		try:
			t = self.addrs[peer]
		except KeyError:
			raise NoSuchAPeerError(peer)
		else:
			return t

	def __contains__(self, peer):
		if peer in self.addrs:
			return True
		else:
			return False

	def isIn(self, peer):
		try:
			self.addrs[peer]
		except KeyError:
			return False

		return True

	def getAllPeers(self):
		return self.addrs.itervalues()

	def anotatePeer(self, peer, attribute, value):
		self.getPeer(peer).setAnotation(attribute, value)

	def getAnotation(self, peer, attribute):
		return self.getPeer(peer).getAnotation(attribute)
