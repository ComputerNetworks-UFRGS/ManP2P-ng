# -*- coding: utf-8 -*-

from overlay import addFunctionInList
from overlay import addHaltFunction as overlayAHF
from overlay import addClientBootstrapFunction as overlayCBF

from peerlist import PeerAlreadyInListError

from commandLineParser import CommandLineParser

from commonRegex import HostNameRegex

bootstrapFunction = [ ]
haltFunction = [ ]

clientBootstrapFunction = [ ]
serverBootstrapFunction = [ ]

def extensionName():
	return str("BasicOverlay")

def addBootstrapFunction(bsfunction, push=False):
	addFunctionInList(bootstrapFunction, bsfunction, push)

def addClientBootstrapFunction(bsfunction):
	addFunctionInList(clientBootstrapFunction, bsfunction)

def addServerBootstrapFunction(bsfunction):
	addFunctionInList(serverBootstrapFunction, bsfunction)

def addHaltFunction(hfunction):
	addFunctionInList(haltFunction, hfunction)

def connectionTokenParser(sParser, t):
	if t[0] == 'c':
			sParser.remotePeerNick = t[2]

	if t[1] == 'n':
		try:
			p = sParser.plist.addPeer(sParser.transport.getPeer(),
									  t[2],
									  sParser)

		except PeerAlreadyInListError:
			sParser.write('b:e:alreadyConnected')
			sParser.disconnect()

			return

		sParser.write('c:a:' + sParser.myNick + ':' +
						','.join(map(str, CommandLineParser().
														getArguments().lport)))

		for f in serverBootstrapFunction:
			f(sParser)

	elif t[1] == 'a':
		try:
			p = sParser.plist.addPeer(sParser.transport.getPeer(),
									  t[2],
									  sParser)

		except PeerAlreadyInListError:
			sParser.write('b:e:alreadyConnected')
			sParser.disconnect()

			return

		for f in clientBootstrapFunction:
			f(sParser)

	elif t[1] == 'e' and t[2] == 'unknownCommand':
		pass

	else:
		sParser.write('b:e:parametersMismatch')
		sParser.disconnect()

	p.setAnotation('lports', map(int, t[3].split(',')))
	sParser.remotePeer = p

	for f in bootstrapFunction:
		f(sParser)

def errorTokenParser(sParser, t):
	pass

def requestConnection(overlay):
	overlay.sParser.write('c:n:' + overlay.factory.myNick+ ':'+
							','.join(map(str, CommandLineParser().
														getArguments().lport)))

def removeFromPeerList(overlay):
	if overlay.sParser and overlay.sParser.remotePeerNick:
		overlay.sParser.plist.removePeer(overlay.sParser.remotePeerNick)

def extendProtocol(lexicalFactory, syntaxFactory):

	startS = lexicalFactory.getState('start')
	establishedS = lexicalFactory.addState('established')

	lexicalFactory.addState('finished')

	syntaxFactory.addToken('c', connectionTokenParser)
	syntaxFactory.addToken('e', errorTokenParser)

	startS.addTransition('established','c:(a|e|n):' +
						 HostNameRegex +':([0-9]+,?)+',
						 lambda t: t.split(':'))

	establishedS.addTransition('finished', 'b:(o|e):[a-zA-Z]+',
								lambda t: t.split(':'))

	establishedS.addTransition('established','p:(n|u):[0-9]+\.[0-9]+',
								lambda t: t.split(':'))

	establishedS.addTransition('established','e:.+',
								lambda t: t.split(':'))

	overlayAHF(removeFromPeerList)
	overlayCBF(requestConnection)

	return True