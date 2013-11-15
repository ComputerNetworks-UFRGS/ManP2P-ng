# -*- coding: utf-8 -*-

#from twisted.internet import reactor

#from wind import AbstractTCPWind, WindDescriptor, WindManager
#from wind import TCPWindFactory, UDPWindFactory
#from message import ExpatError, MessageKeeper

#from xml.dom.minidom import parseString

#from sys import exit

cfg = '''<?xml version="1.0" ?>
<ManP2P-ng subject="configuration">
	<node name="Alderan" domain="redes.inf.ufrgs.br"/>
	<winds>
		<wind address="127.0.0.1" kind="IPv4/UDP" module="pureudp" class="PureUDPProto" ports="8001,8002" protocol="default"/>
		<wind address="127.0.0.1" kind="IPv4/TCP" module="puretcp" class="PureTCPProto" ports="8001,8002" protocol="default"/>
	</winds>
	<overlay cacheSize="40" module="cyclon" class="Cyclon"/>
	<bootstrap>
		<introducer address="127.0.0.1" port="8004" kind="IPv4/UDP" protocol="default"/>
		<introducer address="127.0.0.1" port="8005" kind="IPv4/TCP" protocol="default"/>
	</bootstrap>
</ManP2P-ng>
'''
#<wind address="127.0.0.1" kind="IPv4/TCP" module="httpdtn" class="HttpDTNProto" ports="8001,8002" protocol="HTTP-DTN"/>
#<wind address="127.0.0.1" kind="UNIX/TCP"  class="PsyclonProto" ports="8001,8002" protocol="psyclonGUI" advertise="False"/>


def main():
	from twisted.internet import reactor

	from configparser import ConfigParser
	from wind import WindManager
	from overlay import OverlayManager
	from message import MessageKeeper
	from extension import ComponentManager
	from extension import PluginManager

	from sys import stderr

	print '-+- Parsing configuration file and CLI -+-'

	cfg = ConfigParser()

	try:
		cfg.parseOptions()
	except RuntimeError as e:
		print >> stderr, ' * Error while parsing configurations'

		for l in str(e).splitlines():
			print >> stderr, '\t>', l

		return -1

	except Warning as e:
		errors = str(e).splitlines()

		print >> stderr, ' *', errors[0]
		for l in errors[1:]:
			print >> stderr, '\t>', l

	print '-+- Loading Winds -+-'

	try:
		WindManager.load(cfg['winds'])

	except RuntimeError as e:
		print >> stderr, ' * Error while loading winds:'

		for l in str(e).splitlines():
			print >>  stderr, '\t>', l

	wdesc = WindManager.getDescriptors()

	if len(wdesc) == 0:
		print >> stderr, " * No wind was loaded. Exiting with error."
		return -1

	print '-+- Loading Overlay -+-'

	try:
		OverlayManager.load(cfg['overlay'])

	except RuntimeError as e:
		print >> stderr, (' * Error while loading overlay\n\t' + str(e))
		return -1

	print '-+- Loading Management Components -+-'

	try:
		ComponentManager.load(cfg['managementComponents'])

	except RuntimeError as e:
		print >> stderr, (
			' * Error while loading management component\n\t' + str(e))
		return -1

	overlay = OverlayManager.getOverlay()
	overlay.start(cfg['node'], WindManager.getDescriptors())

	MessageKeeper.startToKeep()
	WindManager.startListening(overlay)
	ComponentManager.register(overlay)

	if len(cfg['bootstrap']) > 0:
		reactor.callWhenRunning(overlay.bootstrap, None, cfg['bootstrap'])

	reactor.run()

if __name__ == '__main__':
	print '+-+--+--+--+--+--+--+--+--+--+--+--+--+-+'
	print '|+- ManP2P-ng (Experimental Version)  -+|'
	print '+-+--+--+--+--+--+--+--+--+--+--+--+--+-+', '\n'

	main()

	#
	# The following codes are highly experimental and will be placed in more
	# appropriated places as soon as they are fully functional.
	#

	#print '-+- Parsing configuration -+-'
	#try:
		#configuration = parseString(cfg)
	#except ExpatError as e:
		#print ' * Bug in config file:\n ->', e
		#exit(255)

	#abort = False

	## # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
	## Load and verification of fields that have just one entry in the
	## configuration file
	#singleFields = [ 'winds', 'overlay', 'node', 'bootstrap' ]
	#d = { }

	#for i in singleFields:
		#print ' |-> Checking for', i, 'entry'
		#d[i] = configuration.getElementsByTagName(i)

		#if len(d[i]) > 1:
			#print ' * More than one', i, 'tag in configuration file. Aborting.'
			#abort = True

		#elif len(d[i]) < 1:
			#print ' * No', i, 'configuration found. Aborting.'
			#abort = True
	#else: print ''

	#if abort is True:
		#exit(255)

	## Make the fields parsed before available
	#for k in d:
		#globals()[k] = d[k][0]

	#print '-+- Loading Node settings -+-'
	## # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
	## Showing system information
	#for i in [ 'name', 'domain' ]:
		#print ' |->', i + ':', node.getAttribute(i)

	#else: print ''

	## # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
	## Parsing winds configuration entries
	#windsToLoad = [ ]

	#windsAttributes = [
		#'address', 'kind',
		#'module', 'class',
		#'ports', 'protocol'
	#]

	#for w in winds.getElementsByTagName('wind'):
		#d = { }

		#for k in w.attributes.keys():
			#d[str(k)] = str(w.getAttribute(k))

		#for k in windsAttributes:
			#try:
				#d[k]

			#except KeyError as e:
				#print ' * Required attribute', e, 'is empty'
				#abort = True

		#d['ports'] = tuple(map(int, d['ports'].split(',')))

		#windsToLoad.append(d)

	#if abort is True:
		#exit(128)

	#print '-+- Loading Winds -+-'
	## # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
	## Loading the winds
	#loadedWinds = [ ]

	#for w in windsToLoad:
		#try:
			#m = getattr(
					#__import__('winds.' + w['module'], fromlist=['*']),
					#w['class'])

			#print ' |-> Wind',w['class'],'loaded'

		#except ImportError as e:
			#print " * Couldn't load wind", w['class']
			#print " *", e
			#abort = True

		#WindManager.addDescriptor(WindDescriptor(w,m))
	#else: print ''

	#print '-+- Loading Overlay -+-'
	#try:
		#o = getattr(
			#__import__(
				#'overlays.' + overlay.getAttribute('module'), fromlist=['*']),
			#overlay.getAttribute('class'))
	#except ImportError as e:
		#print 'N\'overlay:', e
		#exit(32)

	#theOverlay = o()
	#theOverlay.start(node, WindManager.getDescriptors())

	#print ' |-> Overlay name:', theOverlay.getDescription()[0]
	#print ' |-> Class name:', o, '\n'

	#print '-+- Starting the winds -+-'
	## # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
	## Stating the winds
	#for w in WindManager.getDescriptors():
		#print ' | Starting to listen at', w.address
		#print ' |-> Ports:', w.ports
		#print ' |-> Kind:', w.kind
		#print ' |-> Protocol:', w.protocol
		#print ' |-> Class name:', w.wind, '\n'
		#w.listen(theOverlay)

	#if abort is True:
		#exit(64)

	#print '-+- Bootstraping into the network -+-'
	## # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
	##
	#bootstrapNodes = [ ]

	#for i in bootstrap.getElementsByTagName('introducer'):
		#print ' | Node:', i.getAttribute('address')
		#print ' |-> Port:', i.getAttribute('port')
		#print ' |-> Kind:', i.getAttribute('kind')
		#print ' |-> Protocol:', i.getAttribute('protocol'), '\n'

		#bootstrapNodes.append({
			#'address': i.getAttribute('address'),
			#'port': int(i.getAttribute('port')),
			#'kind': i.getAttribute('kind'),
			#'protocol': i.getAttribute('protocol')
		#})

	#def pok():
		#print '-+- Twisted Reactor is staring -+-'
		#print ' |-> Done starting reactor', '\n'

	#MessageKeeper.startToKeep()
	#reactor.callWhenRunning(pok)
	#reactor.callWhenRunning(theOverlay.bootstrap, None, bootstrapNodes)

	#reactor.run()

	"""
	Test messages:
		<message dst="polismassa" kind="hello" nbr="123456" src="alderan"/>
	"""
