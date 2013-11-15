# -*- coding: utf-8 -*-

from random import sample, randint
from xml.dom.minidom import Document
from utils.misc import IAnnotable

class ConnectionAlreadyInSetError(Exception):
	pass

class BogusMessageError(Exception):
	pass

class Peer(IAnnotable):
	# Attributes which are essential for a XML connection description
	connectionAttributes = ['addr', 'port', 'kind', 'protocol']

	def __init__(self, name, transport=None):
		self.name = name
		self.transport = transport

		self.annotations = { }
		self.connections = { }

	def addConnection(self, addr, port, kind='IPv4/UDP', proto='default'):
		if (addr, port, kind) not in self.connections:
			self.connections[(addr,port,kind)] = proto
			return self

		raise ConnectionAlreadyInSetError

	def addConnectionsFromXML(self, connections):
		for c in connections:
			if c.nodeName != 'connection':
				continue

			d = { }

			for e in Peer.connectionAttributes:
				d[e] = c.getAttribute(e)

				if d[e] is '':
					raise BogusMessageError,(
							'Missing essential attribute in a '
							' connection node: ' + e)

			self.addConnection(
				d['addr'], int(d['port']), d['kind'], d['protocol'])

		return self

	def getConnections(self, address=None, port=None, protocol=None, kind=None):
		f = lambda v,k: lambda a: v == a[k]
		r = [ ]

		for k,v in self.connections.iteritems():
			if address != None and address != k[0]:
				continue
			elif port != None and port != k[1]:
				continue
			elif kind != None and kind != k[2]:
				continue
			elif protocol != None and protocol != v:
				continue

			r.append(k + (v,))

		return r

	def getConnectionsXML(
		self, address=None, port=None, protocol=None, kind=None):
		
		r = [ ]

		for t in self.getConnections(address, port, protocol, kind):
			m = Document().createElement('connection')
			m.setAttribute('addr', t[0])
			m.setAttribute('port', str(t[1]))
			m.setAttribute('kind', t[2])
			m.setAttribute('protocol', t[3])

			r.append(m)

		return r

	def getName(self):
		return self.name

	def getTransport(self):
		return self.transport

	def setTransport(self, transport, cback=None, *args, **kw):
		self.transport = transport

		if cback is not None:
			args = args or ()
			kw = kw or {}

			cback(*args, **kw)

		return self

	def setAnnotation(attr, value):
		self.annotations[attr] = value

	def __eq__(self, peer):
		if isinstance(peer, Peer):
			if self.name == peer.getName():
				return True

		elif isinstance(peer, str) or isinstance(peer, unicode):
			if peer == self.name:
				return True

		return False

	def __getattr__(self, attr):
		try:
			return self.__dict__['annotations'][attr]
		except KeyError:
			raise AttributeError, attr

	def __setattr__(self, attr, value):
		shallWeAnnotate = (
			(attr not in self.__dict__) and
			('annotations' in self.__dict__) and
			(attr in self.annotations)
		)

		if shallWeAnnotate is True:
			self.annotations[attr] = value

		self.__dict__[attr] = value

	def __repr__(self):
		if self.transport is None:
			return '<%s None None>' % (self.getName())

		return '<%s %s %s>' % (self.getName(),
								self.transport.peer,
								self.transport.port)

def NoSuchAPeerError(Exception):
	pass

class PeerList(list):
	sortingKey = None

	def __init__(self, *args):
		list.__init__(self, *args)
		self.sort(key=self.sortingKey())

	def append(self, v):
		list.append(self, v)
		self.sort(key=self.sortingKey())

	def addPeer(self, peer):
		raise NotImplementedError

	def sortingKey(self):
		raise NotImplementedError

	def connectionLostCBack(self, reason):
		raise NotImplementedError

	def getSubset(self, size):
		return self.__subinit(sample(self, min(size, len(self))))

	def getRandomPeer(self):
		try:
			return self[randint(0, len(self)-1)]
		except ValueError:
			return None

	def __getitem__(self, key):
		if isinstance(key, int):
			return list.__getitem__(self, key)

		elif isinstance(key, str) or isinstance(key, unicode):
			for i in self:
				if i.getName() == key:
					return i
			else: raise NoSuchAPeerError, key

		return list.__getitem__(self, key)

	def __contains__(self, item):
		if isinstance(item, str) or isinstance(item, unicode):
			name = item
		elif isinstance(item, Peer) is True:
			name = item.getName()
		else:
			raise NotImplementedError, type(item)

		for i in self:
			if i.getName() == name:
				return True

		return False

	def __add__(self, other):
		if isinstance(other, list):
			return list.__add__(self, other)
		else:
			return self + [ other ]

	def remove(self, item):
		c = 0

		for i in self:
			if i == item:
				return self.pop(c)

			c += 1

		raise ValueError, str(item) + ' not in list'

	def __subinit(self, *args):
		"""
		This method creates a new subclass of AbstractPeerList using the
		subclass constructor. Very wise.
		"""
		r = self.__new__(type(self))
		r.__init__(*args)

		return r

	# It should be called __sub__, as it is the Python built-in method for
	# subtractions. However for some mysterious motivation it doesn't work =(
	def drop(self, other):
		scrapList = self.__subinit(self)
		removeCandidates = None

		if isinstance(other, list):
			removeCandidates = other
		elif isinstance(other, Peer) or isinstance(other, str):
			removeCandidates = [ other ]
		else:
			raise NotImplementedError, type(other)

		for p in scrapList:
			if p in removeCandidates:
				scrapList.remove(p)

		return scrapList
