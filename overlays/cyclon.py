# -*- coding: utf-8 -*-

from twisted.python.failure import Failure
from twisted.internet.task import LoopingCall

from message import Message, ExpatError, MessageKeeper
from wind import WindManager, BogusMessageError, NoSuchAWindError
from overlay import AbstractOverlay, KeepProcessing

from cyclonpeer import CyclonPeer, CyclonPeerList, PeerAlreadyInListError

from utils.collectibles import TimedDict

from sys import stderr
from time import time
from random import randint

class Cyclon(AbstractOverlay):

	def getDescription(self):
		return "Cyclon Overlay", ""

	def start(self, properties, winds):
		Cyclon.myself = CyclonPeer(properties['name'])

		for w in winds:
			for p in w.ports:
				Cyclon.myself.addConnection(w.address, p, w.kind, w.protocol)

		self.exchangeLoopControl = LoopingCall(
			self.exchange
		).start(
			self.getExchangeInterval(), False)

	'''
	Bootstrapping related methods
	'''
	bootstrapws = TimedDict(10, 10)

	def doBootstrap(self, transport, peers):
		bootstrap = Message('bootstrap', self.myself.getName())

		for c in self.myself.getConnectionsXML():
			bootstrap.getBody().appendChild(c)

		bootstrap.addErrback(self.bootstrap, peers=peers)
		bootstrap.addCallback(lambda m: True)
		bootstrap.send(transport, 3, 3)

	def bootstrapReceived(self, (transport, message)):
		print '-+- Someone is trying to join the overlay -+-'
		print ' | Name:', message.getSource()
		print ' | Address:', transport.getPeer().host
		print ' | Port:', transport.getHost().port
		print ' | Kind:', transport.getPeer().type, '\n |'

		# This message if for acknowledging the new peer that we received
		# it's bootstrap message.
		Message('reply',
			source=Cyclon.myself.getName(), destination=message.getSource(),
			extraAttributes={ 'for': message.getNumber() }
		).send(
			transport
		).addBoth(
			lambda m: True)

		# Maybe our first acknowledge messages hasn't arrive at the new peer
		# So, if it's the case, we shall only send him the acknowledging,
		# which was done in the previous instructions.
		if message.getSource() in Cyclon.bootstrapws:
			return

		# Marking that we received the bootstrap message.
		Cyclon.bootstrapws[message.getSource()] = True

		# Creating a new peer to easy the messages building
		peer = CyclonPeer(message.getSource(), transport)
		peer.addConnectionsFromXML(message.getBody().childNodes)

		# If we have no other neighboring peer, we shall add this guy to our
		# peer list.
		if len(Cyclon.peerList) is 0:
			print ' | we have no peer, so adding him.'
			self.replyIntroduction(peer, Cyclon.myself)

		# Or, if we have some other fellows, we send them an introductory
		# message.
		else:
			self.sendIntroductions(peer, Cyclon.peerList.getSubset())

	def replyIntroduction(self, peer, introduced):
		replyMessage = Message('intro reply',
			source=Cyclon.myself.getName(), destination=peer.getName(),
			annotations={ 'introduced': introduced })

		node = replyMessage.body.appendChild(Message.createElement("node"))
		node.setAttribute('name', introduced.getName())

		for c in introduced.getConnectionsXML():
			node.appendChild(c)

		replyMessage.send(
			peer.transport, 5, 3
		).addCallback(
			lambda m: (
				Cyclon.peerList.addPeer(peer),
				(Cyclon.peerList.remove(m.introduced) or True
					if m.introduced in Cyclon.peerList else True)
			)
		).addErrback(lambda m: True)

	def sendIntroductions(self, peer, candidatesSet, replacersSet=None):
		# Building the newcomers peer' description
		nodeInfo = Message.createElement('node', { 'name':peer.getName() })

		for c in peer.getConnectionsXML():
			nodeInfo.appendChild(c)

		# The replacersSet is used for replace unreachable peers in the
		# errback method 'introductionErrback'.
		if replacersSet == None:
			replacersSet = Cyclon.peerList.drop(candidatesSet)

		print ' | Sending introduction to the following peers: '
		# Sending the introductory message to the random candidatesSet
		for q in candidatesSet:
			# One message per peer as the destination must be explicit
			introMessage = Message(kind='introduction',
				source=Cyclon.myself.getName(),
				destination=q.getName(),
				extraAttributes={
					'ttl': str(self.getTTL())},
				annotations={
					'candidatesSset': candidatesSet,
					'newcomer': peer,
					'replacersSet': replacersSet})

			# Appending newcomers info
			introMessage.getBody().appendChild(nodeInfo)

			print ' |->', q.getName()
			# The actual sending
			introMessage.send(
				q.transport, 3, 3
			).addCallback(
				lambda m: True
			).addErrback(
				self.introductionErrback, message=introMessage)

	introductions = TimedDict(10, 10)

	def introductionReceived(self, (transport, message)):
		try:
			ttl = int(message.getBody().getAttribute("ttl")) - 1
		except ValueError:
			print >> stderr, (
				' * Missing or not numerical TTL field'
				' in a introduction message')
			return

		# If transport is None, we have already received this introduction and
		# passed it forward but the peer chosen was not reached. So, we shall
		# not halt processing now, but select a new peer in our peer list. Of
		# course, the falling one was removed from the list by the error
		# callback function.
		if transport is not None:
			# Sending the acknowledge of the introduction
			Message('reply',
				source=Cyclon.myself.getName(),
				destination=message.getSource(),
				extraAttributes={'for': message.getNumber()}
			).send(transport, 0)

			mID = (ttl, message.getNumber(), message.getSource())

			# Verifying if this message was. If it was, we do not need to
			# process it again
			if mID in Cyclon.introductions:
				return

			Cyclon.introductions[mID] = True

		if ttl > 0 and len(Cyclon.peerList) > 0:
			message.addAnnotation('nextHop', Cyclon.peerList.getRandomPeer())

			Message('introduction',
				source=Cyclon.myself.getName(),
				destination=message.nextHop.getName(),
				extraAttributes={'ttl': str(ttl)}
			).appendToBody(
				message.getBody().firstChild
			).send(
				message.nextHop.transport, 3, 3
			).addCallback(
				lambda m: True
			).addErrback(
				self.introRecvdErrback, message=message)

		else:
			node = message.getElement('node')

			peer = CyclonPeer(node.getAttribute('name'))
			peer.addConnectionsFromXML(node.childNodes)

			winds = [ ]

			for c in peer.getConnections():
				try:
					w = WindManager.findWind(c[2], c[3])
				except NoSuchAWindError:
					continue

				winds.append((w,c))

			if len(Cyclon.peerList) > 0:
				introduced = Cyclon.peerList.getRandomPeer()
			else:
				introduced = Cyclon.myself

			self.introductoryConnection(None, peer, introduced, winds)

	def introreplyReceived(self, (transport, message)):
		try:
			node = message.getElement('node')
		except IndexError:
			print >> stderr, ' * Introduction reply without node information'
			return

		Message('reply',
			source=Cyclon.myself.getName(),
			destination=message.getSource(),
			extraAttributes={'for': message.getNumber()}
		).send(
			transport, 0, 0
		).addBoth(
			lambda m: True)

		peer = CyclonPeer(node.getAttribute('name'))
		peer.addConnectionsFromXML(node.childNodes)

		for c in peer.getConnections():
			try:
				wind = WindManager.findWind(c[2],c[3])
			except NoSuchAWindError:
				continue
			
			break

		else:
			return

		wind.factory.connect(
			c[0], c[1]
		).addCallbacks(
			callback=peer.setTransport,
			errback=lambda m: m
		).addCallbacks(
			Cyclon.peerList.addPeer
		).addErrback(
			lambda m: m.trap(PeerAlreadyInListError) or True)

	'''
	Bootstrapping auxiliary methods
	'''
	def introRecvdErrback(self, failure, message):
		if message.nextHop in Cyclon.peerList:
			Cyclon.peerList.remove(message.nextHop)

		self.introductionReceived((None, message))

	def introductoryConnection(self, failure, peer, introduced, winds):
		try:
				wind, connection = winds.pop(0)
		except IndexError:
			print >> stderr, (
				' * Cannot find a way to contact peer %s\n'
				'   It will not receive our introduction.')
			return

		wind.factory.connect(
			connection[0], connection[1]
		).addCallbacks(
			callback=peer.setTransport,
			errback=lambda m: Failure()
		).addCallbacks(
			callback=self.replyIntroduction,
			callbackKeywords={
				'introduced': Cyclon.peerList.getRandomPeer()},
			# Errback settings
			errback=self.introductoryConnection,
			errbackKeywords={
				'peer': peer, 'introduced': introduced, 'winds': winds}
		)

	def introductionErrback(self, fail, message):
		print >> stderr, (
			' * Introduction of %s to %s has failed\n'
		) % (message.newcomer.getName(), message.getDestination())

		# Probably a bug!
		Cyclon.peerList.remove(message.getDestination())

		try:
			replaceCandidate = [ message.replacersSet.pop(0) ]

		except IndexError as e:
			print >> stderr, (
				' * There is no more candidates to introduce this peer')

			if message.getDestination() not in self.peerList:
				print ' | Adding him to our peer list'
				self.replyIntroduction(message.newcomer, Cyclon.myself)

		else:
			self.sendIntroductions(
				message.newcomer,
				message.replaceCandidate,
				message.replacersSet)

	'''
	Membership management code
	'''
	def exchange(self, vault=None):
		'''
		First thing first, the initiator issues a "exchange request".

		In Cyclon protocol, the exchange request consists in generating a
		subset of peers to exchange with the oldest peer. The "age" of a peer
		is based on the how many exchanges have passed since I last updated
		the state of that peer of my neighboring list.
		'''
		if self.itsExchanging():
			return True

		Cyclon.peerList.increaseAges()

		try:
			oldestPeer = Cyclon.peerList.getOldest()

		except IndexError:
			print ' * No peer available for exchanging'
			return True

		print '''Starting an exchange with ''' + oldestPeer.getName()

		self.exchangeStarted()

		print >> stderr, ''' * computing subset '''
		shuffleSize = randint(1, self.getShuffleLength()) - 1
		randomSubset = Cyclon.peerList.drop(oldestPeer).getSubset(shuffleSize)
		sentSet = CyclonPeerList([ oldestPeer ]) + randomSubset

		message = Message(kind='exchange',
			source=Cyclon.myself.getName(),
			destination=oldestPeer.getName())

		randomSubset.insert(0, Cyclon.myself)

		body = message.getBody()

		print >> stderr, ''' * building message '''

		for p in randomSubset:
			node = body.appendChild(message.createElement('node'))
			node.setAttribute('name', p.getName())
			node.childNodes = p.getConnectionsXML()

		print >> stderr, ''' * Sending request '''

		message.send(
			oldestPeer.transport, 3, 3
		).addCallback(
			self.exchangeReplyReceived, sentSet=sentSet
		).addErrback(
			self.removePeerErrback, peer=oldestPeer.getName(), success=False
		).addErrback(
			self.exchangeFinished, success=False
		).addErrback(
			self.exchange)

		return True

	def exchangeReceived(self, (transport, message)):
		'''
		The first method executed by a exchange request receiver.

		This method extract the subset of peers sent by the exchange initiator
		and then generates another subset of length less of equal to the size
		o the subset of the initiator and sends it to him.
		'''
		print message.getSource() + ''' requested an exchange'''

		if self.itsExchanging():
			return message.reply(
						source=Cyclon.myself.getName()
					).send(
						transport)

		self.exchangeStarted()

		receivedSet = [
			CyclonPeer(node.getAttribute('name'), None)
				.addConnectionsFromXML(node.childNodes) for node in message ]

		sentSet = Cyclon.peerList.getSubset(len(receivedSet))

		if len(sentSet) < len(receivedSet):
			receivedSet = receivedSet[:len(sentSet)]

		reply = message.reply(source=Cyclon.myself.getName())
		body = reply.getBody()

		for p in sentSet:
			node = body.appendChild(message.createElement('node'))
			node.setAttribute('name', p.getName())
			node.childNodes = p.getConnectionsXML()

		reply.send(
			transport, 3, 3
		).addCallback(
			self.commitExchange, commitData=(sentSet, receivedSet, False)
		).addErrback(
			self.removePeerErrback, peer=message.getSource(), success=False
		).addErrback(
			self.exchangeFinished)

		return True

	def exchangeReplyReceived(self, (transport, message), sentSet):
		'''
		This method receives the subset of peers of our fellow and then adjust
		uses it to reset the length of the original subset (paranoia) and then
		commit all the exchanges
		'''
		print message.getSource() + ''' reply the exchange'''

		if len(message.getBody().childNodes) < 1:
			print message.getSource() + ''' denied to exchange with us'''
			return self.exchange()

		receivedSet = [
			CyclonPeer(node.getAttribute('name'), None)
				.addConnectionsFromXML(node.childNodes) for node in message ]

		sentSet = sentSet[:len(receivedSet)]

		message.reply(
			source=Cyclon.myself.getName()
		).send(
			transport, 3, 3
		).addCallback(
			self.commitExchange, commitData=(sentSet, receivedSet, True)
		).addErrback(
			self.exchangeFinished, success=False
		).addErrback(
			self.removePeerErrback, peer=message.getSource(), success=False
		).addErrback(
			self.exchange)

		return True

	def commitExchange(self, (transport, message), commitData):
		print '''We are commiting the exchange with ''' + message.getSource()
		sentSet, receivedSet, initiator = commitData

		if initiator is False:
			message.reply(
				source=Cyclon.myself.getName()
			).send(
				transport)

		print '''Received set is ''', receivedSet
		print '''Sent set is ''', sentSet
		print '''Peerlist is ''', Cyclon.peerList

		if Cyclon.myself in sentSet:
			sentSet.remove(Cyclon.myself)

		if Cyclon.myself in receivedSet:
			receivedSet.remove(Cyclon.myself)

		for p in receivedSet:
			if p in Cyclon.peerList:
				continue

			self.sayHello(
				peer=p
			).addCallbacks(
				callback=Cyclon.peerList.exchangePeer,
				callbackKeywords={'outters': sentSet},
				#
				errback=self.removePeerErrback,
				errbackKeywords={'peer': p})

		if message.getSource() in Cyclon.peerList:
			print 'Adjusting peer age'
			Cyclon.peerList[message.getSource()].setAge(0)

		self.exchangeFinished()
		return True

	'''
	Membership management auxiliary methods and attributes
	'''
	shuffleLength = 10

	def setShuffleLength(self, value):
		Cyclon.shuffleLength = value
		return self

	def getShuffleLength(self):
		return Cyclon.shuffleLength

	exchangeInterval = 10

	def setExchangeInterval(self, value):
		Cyclon.exchangeInterval = 10

	def getExchangeInterval(self):
		return Cyclon.exchangeInterval

	exchanging = False

	def exchangeStarted(self):
		self.exchanging = True

	def exchangeFinished(self, failure=None, success=True):
		self.exchanging = False

		if not success:
			return failure

		return True

	def itsExchanging(self):
		return self.exchanging

	peerList = CyclonPeerList()

	def removePeerErrback(self, Failure, peer, success=True):
		print ''' * Removing %s due to Errbacking\n%s''' % (peer, Failure)

		try:
			Cyclon.peerList.remove(peer)
		except ValueError:
			print peer, 'was not in list'
		else:
			print peer, 'successfully removed'

		if not success:
			raise Failure('User requested')

		return True

	def sayHello(self, peer):
		w = None

		for c in peer.getConnections():
			try:
				w = WindManager.findWind(c[2], c[3])
			except NoSuchAWindError:
				pass
			else:
				break
		else:
			print >> stderr, (
				" * Couldn't find a wind for contacting %s" % peer.getName())
			raise NoSuchAWindError, 'No wind for contacting ' + peer.getName()

		return w.factory.connect(
			c[0], c[1] # Address / Port
		).addCallback(
			peer.setTransport
		).addCallback(
			self.sendHello)

	def sendHello(self, peer):
		Message(kind='hello',
			source=Cyclon.myself.getName(), destination=peer.getName()
		).send(
			peer.getTransport(), 3, 3)

		return peer

	def helloReceived(self, (transport, message)):
		message.reply(
			source=Cyclon.myself.getName()
		).send(
			transport)

	'''
	Message passing related code
	'''
	def broadcast(self, subject, inner, replyHandler, *args, **kwargs):
		for p in self.peerList.getSubset(self.getFanOut()):
			Message(
				kind='broadcast', source=Cyclon.myself.getName(),
				extraAttributes={'ttl': self.getTTL(), 'subject': subject},
				innerContent=inner
			).addCallback(
				replyHandler, *args, **kwargs
			).send(
				p.getTransport())

	def broadcastReceived(self, (transport, message)):
		try:
			cttl = int(message.getAttribute('ttl')) - 1
		except:
			return False

		if cttl > 0:
			for p in Cyclon.peerList.getSubset(self.getFanOut()):
				MulticastableMessage(
					kind='broadcast', source=message.getSource(),
					extraAttributes={
						'ttl': cttl, 'subject': message.getSubject()},
					innerContent=message.getBody().firstChild
				).addCallback(
					self.backCast, message, transport
				).send(
					p.getTransport())

		raise KeepProcessing

	def backCast(self, (transport, message), original, itsTransport):
		original.reply(
			source=message.getSource(),
			innerContent=message.getBody().firstChild
		).send(
			itsTransport)

	'''
	Message passing auxiliary code
	'''
	fanOut = 5

	def setFanOut(self, fanOut):
		self.fanOut = fanOut
		return self

	def getFanOut(self):
		return self.fanOut

	maxDepth = 5

	def setTTL(self, value):
		self.maxDepth = value
		return self

	def getTTL(self):
		return self.maxDepth
