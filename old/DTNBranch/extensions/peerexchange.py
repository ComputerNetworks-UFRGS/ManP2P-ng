# -*- coding: utf-8 -*-
#                                                                           80

from twisted.internet import reactor

from overlay import listenPort

from peerlist import NoSuchAPeerError
from peerlist import PeerAlreadyInListError

from extensionLoader import ExtensionLoader

from commonRegex import IPHostRegex, HostNameRegex

tstring	= 'peerlist'
request	= 'r'
awnser	= 'a:[1-9][0-9]{0,2}\.[0-9]{1,3}\.[0-9]{1,3}\.[1-9][0-9]{0,2}:[a-zA-Z]+'
delete	= 'd:'

er = tstring + ':(r|e|a:'+IPHostRegex+':([0-9]+,?)+:'+HostNameRegex+':[0-9]*)'

def connectToPeer(sParser, peer, portList):
	if peer[1] in sParser.plist:
		print "Stopping cause we already  got a connection"
		return
	elif portList == []:
		return

	print "Trying " + peer[1] + " trough " + str(portList[0])

	reactor.connectTCP(peer[0], portList[0],
						sParser.factory.overlay.connectorFactory, 3)

	reactor.callLater(3, connectToPeer, sParser, peer, portList[1:])

	del portList
	del peer


def tConsumer(sParser, t):
	if t[0] != tstring:
		if sParser.remotePeerNick in __peer__state:
			del __peer__state[sParser.remotePeerNick]

		sParser.write('b:e:parametersMismatch')
		sParser.disconnect()

	if t[1] == 'r':
		sendPeerList(sParser)

	elif t[1] == 'a':
		if t[4] not in sParser.plist:
			connectToPeer(sParser, (t[2],t[4]), map(int, t[3].split(',')))

	elif t[1] == 'e':
		print ("Host " + sParser.remotePeerNick + " doesn't understand " + extensionName())

def requestPeerList(sParser):
	sParser.remotePeer.setAnotation('distance', [ (0, None) ])
	sParser.write(tstring + ':r')

	return True

def getMinRoute(peer):
	return min(peer.getAnotation('distance'))

def sendPeerList(sParser):
	for i in sParser.plist.getAllPeers():
		if i.nick != sParser.remotePeerNick:
			ports = ','.join(map(str,i.getAnotation('lports')))
			sParser.write('peerlist:a:' + i.peer + ':' + ports + ':' +
							i.nick + ':' + str(getMinRoute(i)[0] + 1))

def extensionName():
	return "PeerExchange"

def returnTokens(t):
	return tuple(t.split(':'))

def extendProtocol(lParserFactory, sParserFactory):
	if not ExtensionLoader().isActive("BasicOverlay"):
		return False

	sParserFactory.addToken(tstring, tConsumer)
	(ExtensionLoader().getExtension("BasicOverlay").
										addBootstrapFunction(requestPeerList))

	(lParserFactory.
			getState('established').
				addTransition('established', er, returnTokens))

	return True
