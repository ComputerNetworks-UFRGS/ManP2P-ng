# -*- coding: utf-8 -*-

from twisted.internet.task import LoopingCall
from twisted.internet.defer import Deferred
from twisted.internet import reactor

from xml.dom.minidom import Document, parseString
from xml.parsers.expat import ExpatError

from time import time
from random import randint

class MessageKeeper:
	RMIN = 2**10
	RMAX = 2**20

	MESSAGE_TIMEOUT = 10

	nbrs = { }

	started = False
	cleanner = None

	@staticmethod
	def startToKeep():
		if MessageKeeper.cleanner is not None:
			return

		MessageKeeper.cleanner = LoopingCall(
									MessageKeeper.keepingFunction
								).start(3, False)

		return MessageKeeper.cleanner

	@staticmethod
	def keepingFunction():
		toDel = [ ]

		for k,i in MessageKeeper.nbrs.iteritems():
			if i[1].timeoutCall is not None:
				continue

			if time() - i[0] > MessageKeeper.MESSAGE_TIMEOUT:
				toDel.append(k)

		for k in toDel:
			del MessageKeeper.nbrs[k]

	@staticmethod
	def getFreeNumber(message):
		nbr = ''

		while True:
			nbr = str(randint(MessageKeeper.RMIN, MessageKeeper.RMAX))

			if nbr not in MessageKeeper.nbrs:
				MessageKeeper.nbrs[nbr] = (time(), message)
				break

		return nbr

	@staticmethod
	def getMessage(number):
		return MessageKeeper.nbrs[number][1]

	@staticmethod
	def callback((transport, message)):
		forAttr = message.getBody().getAttribute('for')

		if MessageKeeper.nbrs[forAttr][1].called == True:
			return

		MessageKeeper.nbrs[forAttr][1].clearTimeout()
		MessageKeeper.nbrs[forAttr][1].callback((transport, message))

class MissingAttributeError(Exception):
	pass

class BasicMessage(Document):
	dummyDoc = Document()

	def __init__(self, data=None):
		Document.__init__(self)

		if data is not None:
			self.__dict__ = parseString(data).__dict__

		self.annotations = { }

	@staticmethod
	def createElement(elementName, attributes={}):
		assert isinstance(elementName, str), "elementName must be a str"
		assert isinstance(attributes, dict), 'attributes must be a dict'

		r = BasicMessage.dummyDoc.createElement(elementName)

		for a,v in attributes.iteritems():
			r.setAttribute(a, v)

		return r

	def addAnnotation(self, annotation, value):
		self.annotations[annotation] = value
		return self

	def appendToBody(self, node):
		self.body.appendChild(node)
		return self

	def getAnnotation(self, annotation):
		return self.annotations[annotation]

	def getElement(self, element):
		return self.getElementsByTagName(element)[0]

	def getBody(self):
		return self.firstChild

	def getAttribute(self, attr):
		if not self.getBody().hasAttribute(attr):
			raise AttributeError, attr

		return self.getBody().getAttribute(attr)

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
			return self.annotations[attr]

		self.__dict__[attr] = value
		return self.__dict__[attr]

class TimeouteableMessage(BasicMessage):
	def __init__(self, data):
		BasicMessage.__init__(self, data)

		self.called = False
		self.timeoutCall = None

	def clearTimeout(self):
		if self.timeoutCall is None:
			return

		if self.timeoutCall.active() is True:
			self.timeoutCall.cancel()

		self.timeoutCall = None

	def hasTimeouted(self, timeoutFunc, *args, **kw):
		if self.called:
			return True

		self.clearTimeout()
		return timeoutFunc(*args, **kw)

	def setTimeout(self, seconds, timeoutFunc, *args, **kw):
		if self.called is True:
			return None

		assert not self.timeoutCall, 'Double timeout call'

		self.timeoutCall = reactor.callLater(
			seconds, self.hasTimeouted, timeoutFunc, *args, **kw
		)

		return self.timeoutCall

class CallbackableMessage(BasicMessage, Deferred):
	def __init__(self, data, canceller=None):
		BasicMessage.__init__(self, data)
		Deferred.__init__(self)

	def reset(self, canceller=None):
		cback = self.callbacks

		canceller = (canceller if canceller is not None
						else self._canceller if self._canceller != None
							else None)

		Deferred.__init__(self, canceller)

		self.callbacks = cback

class Message(TimeouteableMessage, CallbackableMessage):
	def __init__(self, kind=None, source=None, destination=None,
				extraAttributes={}, annotations={}, data=None,
				canceller=None, innerContent=None):

		TimeouteableMessage.__init__(self, data)
		CallbackableMessage.__init__(self, data, canceller)

		# If 'data' is None we are just creating a new message class instance.
		# The methods parameter may be used to set some attributes during the
		# instantiation time.
		if data is None:
			self.body = self.appendChild(self.createElement('message'))
			self.getBody().setAttribute('nbr',
								MessageKeeper.getFreeNumber(self))

			if kind is not None:
				self.setKind(kind)

			if source is not None:
				self.setSource(source)

			if destination is not None:
				self.setDestination(destination)

			if innerContent is not None:
				self.getBody().appendChild(innerContent)

		# This else part is executed when some data (through the method
		# parameter 'data') is passed. This data will be parsed and it content
		# will be inspect in search for a overlay message.
		elif self.getBody().nodeName == 'message':
			nbr = self.getBody().getAttribute('nbr')
			knd = self.getBody().getAttribute('kind')
			src = self.getBody().getAttribute('src')

			if '' in (nbr, knd, src):
				raise MissingAttributeError

		else:
			raise Exception, 'Bad constructor usage'

		for a,v in extraAttributes.iteritems():
			self.getBody().setAttribute(a,v)

		for a,v in annotations.iteritems():
			self.addAnnotation(a,v)

	def reset(self, canceller=None):
		CallbackableMessage.reset(self, canceller)
		TimeouteableMessage.clearTimeout(self)

	def setAttribute(self, attr, value):
		self.getBody().setAttribute(attr, value)
		return self

	def getDestination(self):
		return self.getBody().getAttribute('dst')

	def getKind(self):
		return self.getBody().getAttribute("kind")

	def getNumber(self, transform=str):
		return transform(self.getBody().getAttribute('nbr'))

	def getSource(self):
		return self.getBody().getAttribute('src')

	def getSubject(self):
		return self.getAttribute('subject')

	def setDestination(self, dst):
		return self.setAttribute('dst', dst)

	def setKind(self, kind):
		return self.setAttribute('kind', kind)

	def setSource(self, src):
		return self.setAttribute('src', src)

	def send(self, transport, timeout=0, times=None):
		if transport.raw:
			transport.doSend(self.toxml().encode('utf-8'))
		else:
			transport.doSend(self, self.toxml().encode('utf-8'))

		if times > 0:
			self.reset()
			self.setTimeout(timeout, self.send, transport, timeout, times-1)

		elif times != None:
			self.errback(
				Exception('-+- Message retransmission error -+-\n'
							+ self.toprettyxml()))

		return self

	def reply(self, *args, **kwargs):
		reply = type(self)(kind='reply', *args, **kwargs)

		reply.getBody().setAttribute('for', self.getNumber())
		reply.setDestination(self.getSource())

		return reply

	# The following two methods are used to iterate through the sub nodes of a
	# message, Usually, they are used to process the 'connections' tags during
	# a peer exchange of a Cyclon. But, your creativity shall guide yours
	# thoughts and algorithms =D
	def __iter__(self):
		self.thisSibling = self.getBody().firstChild
		return self

	def next(self):
		if self.thisSibling is not None:
			r = self.thisSibling
			self.thisSibling = self.thisSibling.nextSibling

			return r

		else:
			raise StopIteration

	def __len__(self):
		return len(self.getBody().childNodes)

from copy import copy

class MulticastableMessage(Message):
	def __init__(self, *args, **kwargs):
		Message.__init__(self, *args, **kwargs)

	def callback(self, result):
		copyCat = copy(self)

		copyCat.result = result
		copyCat._runCallbacks()

if __name__ == '__main__':
	from twisted.internet import reactor

	def doitnow():
		MessageKeeper.getFreeNumber(None)

	MessageKeeper()
	LoopingCall(doitnow).start(1)

	reactor.run()
