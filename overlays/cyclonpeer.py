# -*- coding: utf-8 -*-

from peer import Peer, PeerList, NoSuchAPeerError

class CyclonPeer(Peer):
	def __init__(self, name, transport=None):
		Peer.__init__(self, name, transport)
		self.age = 0

	def increaseAge(self):
		self.age = self.age + 1

	def getAge(self):
		return self.age

	def setAge(self, age):
		self.age = age

class PeerAlreadyInListError(Exception):
	pass

class FullPeerListError(Exception):
	pass

CACHE_LENGTH = 2

class CyclonPeerList(PeerList):
	CACHE_LENGTH = CACHE_LENGTH
	TTL = 5

	def hasRoom(self):
		return True if len(self) < self.CACHE_LENGTH else False

	def addPeer(self, peer):
		if self.hasRoom() is False:
			raise FullPeerListError

		if peer.getName() in self:
			raise PeerAlreadyInListError, peer.getName()

		if peer.transport != None:
			peer.transport.addCallback(self.connectionLostCBack, peer=peer)

		self.append(peer)

		return self

	def exchangePeer(self, peer, outters):
		if not self.hasRoom():
			self.remove(outters.pop(0))

		self.addPeer(peer)

	def increaseAges(self):
		for i in self:
			i.increaseAge()

	def connectionLostCBack(self, reason, peer):
		peer.setTransport(None)

	def sortingKey(self):
		return lambda t: t.age

	def increaseAge(self):
		for i in self:
			i.increaseAge()

		return self

	def getOldest(self):
		return self[-1]

	def getSubset(self, size=CACHE_LENGTH):
		return PeerList.getSubset(self, size)

	def setCacheSize(self, newSize):
		if newSize < len(self):
			raise Exception, (
				"new cache length is lower than the current list size")

		self.CACHE_LENGTH = newSize
