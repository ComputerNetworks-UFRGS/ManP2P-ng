# -*- coding: utf-8 -*-

from twisted.python.usage import Options

from xml.dom.minidom import parse as XMLParser
from xml.parsers.expat import ExpatError

from sys import exit, stderr

from re import match

def raiseMatch(re, data):
	if match(re, data) is None:
		raise AssertionError, data + ' did not match ' + re

	return True

class ConfigParser(Options):
	__cfoverride = ' (Overrides configuration file data)'

	optParameters = [
		['bootstrap', 'b', None, 'Bootstrap node' + __cfoverride],
		['config-file', 'c', None, 'Node configuration file'],
	]

	winds   = [ ]
	node    = { }
	overlay = { }
	components = [ ]

	def __init__(self):
		Options.__init__(self)

		self['winds'] = ConfigParser.winds
		self['node'] = ConfigParser.node
		self['overlay'] = ConfigParser.overlay
		self['components'] = ConfigParser.components


	def subElementsParser(self, container, elementName, requiredAttributes=[]):
		errors = [ ]
		cfName = str(container.nodeName)

		if cfName not in self:
			setattr(ConfigParser, cfName, [ ])
			self[cfName] = getattr(ConfigParser, cfName)

		for e in container.getElementsByTagName(elementName):
			d = { }

			for k in e.attributes.keys():
				d[str(k)] = str(e.getAttribute(k))

			for k,t,f in requiredAttributes:
				try:
					assert d[k] is not None, (
						'Required attribute %s is missing' % (k))

					if f is not None:
						f(d[k])

					if t is not None:
						d[k] = t(d[k])

				# continue expression implicit here =)
				except KeyError as e:
					errors.append('Required attribute %s is empty' % (k))

				except AssertionError as e:
					errors.append(str(e))

			if len(errors) == 0:
				self[cfName].append(d)

		if len(errors) > 0:
			raise RuntimeError, str.join('\n', errors)

	def elementParser(self, container, requiredAttributes):
		errors = [ ]
		nodeName = str(container.nodeName)

		if nodeName not in self:
			setattr(ConfigParser, nodeName, { })
			self[nodeName] = getattr(ConfigParser, nodeName)

		for k in container.attributes.keys():
			self[nodeName][str(k)] = str(container.getAttribute(k))

		for k,t,f in requiredAttributes:
			try:
				assert self[nodeName][k] is not None, (
					'Required attribute %s is missing' % (k))

				if f is not None:
					f(self[nodeName][k])

				if t is not None:
					self[nodeName][k] = t(self[nodeName][k])

			except KeyError as e:
				errors.append('Required attribute %s is empty' % (k))

			except AssertionError as e:
				errors.append(str(e))

		if len(errors) > 0:
			raise RuntimeError, str.join('\n', errors)

	def opt_node(self, symbol):
		symbol = symbol.split(',')

		try:
			self['node']['name'], self['node']['domain'] = symbol[0].split('@')
		except Exception as e:
			raise RuntimeError, 'Error parsing node options\n\t' + str(e)

		for r in symbol[1:]:
			t = r.split(':')
			self['node'][t[0]] = str.join(':', t[1:])

	opt_n = opt_node

	def nodeParser(self, node):
		for k in node.attributes.keys():
			self['node'][str(k)] = str(node.getAttribute(k))

		if 'name' not in self['node'] or 'domain' not in self['node']:
			raise RuntimeError, 'Missing node information'

	# address,port,kind,protocol,module,class,extra0:extraData0
	def opt_winds (self, symbol):
		symbol = symbol.split(',')
		wind = None

		try:
			wind = {
				'address': symbol[0],
				'ports': (int(symbol[1]),),
				'kind': symbol[2],
				'protocol': symbol[3],
				'module': symbol[4],
				'class': symbol[5],
			}

		except IndexError:
			raise RuntimeError, 'Missing argument in wind CLI tuple'

		for r in symbol[6:]:
			k = r.split(':')
			wind[k[0]] = str.join(':', k[1:])

		self['winds'].append(wind)

	opt_w = opt_winds

	def windsParser(self, winds):
		requiredAttributes = [
			('address', None, None),
			('kind', None, None),
			('module', None, None),
			('class', None, None),
			('ports', lambda s: tuple(map(int, s.split(','))), None),
			('protocol', None, None),
		]

		self.subElementsParser(winds, 'wind', requiredAttributes)

	def opt_overlay(self, symbol):
		symbol = map(str, symbol.split(','))

		try:
			self['overlay']['module'], self['overlay']['class'] = symbol[:2]

			raiseMatch('[_a-zA-Z]+(\.[_a-zA-Z]+)*', self['overlay']['module'])
			raiseMatch('[_a-zA-Z]+', self['overlay']['class'])

		except ValueError:
			raise RuntimeError, (
				'Error reading overlay configuration from CLI')

		except AssertionError as e:
			raise RuntimeError, str(e)

		for r in symbol[2:]:
			k = r.split(':')
			self['overlay'][k[0]] = str.join(':', k[1:])

	opt_o = opt_overlay

	def overlayParser(self, overlay):
		requiredAttributes = [
			('module',
				None, lambda s: raiseMatch('[_a-zA-Z]+(\.[_a-zA-Z]+)*', s)),
			('class',
				None, lambda s: raiseMatch('[_a-zA-Z]+', s)),
		]

		self.elementParser(overlay, requiredAttributes)

	def componentsParser(self, components):
		requiredAttributes = [
			('name', None, None),
			('class', None, None)
		]

		self.subElementsParser(components, 'component', requiredAttributes)

	def bootstrapParser(self, bootstrap):
		# TODO: validation of these fields
		requiredAttributes = [
			('address', None, None),
			('port', lambda p: int(p), None),
			('kind', None, None),
			('protocol', None, None),
		]

		self.subElementsParser(bootstrap, 'introducer', requiredAttributes)

	def postOptions(self):
		cdata = None

		if 'config-file' in self and self['config-file'] != None:
			try:
				cdata = XMLParser(self['config-file'])

			except ExpatError as ee:
				raise RuntimeError, (
					'Error while parsing configuration file\n' + str(ee))

		mustHaveFields = [{
			'nm': 'winds',
			'prsr': self.windsParser,
		},{
			'nm': 'overlay',
			'prsr': self.overlayParser,
		},{
			'nm': 'node',
			'prsr': self.nodeParser,
		}]

		optionalFields = [{
			'nm': 'bootstrap',
			'prsr': self.bootstrapParser,
			'dval': [ ]
		},{
			'nm': 'managementComponents',
			'prsr': self.componentsParser,
			'dval': [ ]
		}]

		for f in mustHaveFields:
			if len(self[f['nm']]) is 0 and cdata is not None:
				try:
					f['prsr'](cdata.getElementsByTagName(f['nm'])[0])

				except RuntimeError as e:
					raise RuntimeError, (
						'While parsing %s\n\t%s' % (f['nm'], str(e)))

			elif len(self[f['nm']]) is 0 and cdata is None:
				raise RuntimeError, (
					'%s configuration file section\'s or CLI flag not found\n'
					'Review your setup and try again.' % (f['nm']))

		for f in optionalFields:
			if f['nm'] not in self or self[f['nm']] is None:
				self[f['nm']] = f['dval']

			if cdata is None:
				continue

			try:
				f['prsr'](cdata.getElementsByTagName(f['nm'])[0])

			except IndexError as ie:
				print ' > %s not in configuration file' % (f['nm'])
			except Exception as e:
				print >> stderr, (
					' * While parsing a optional parameter %s\n * %s' % (
						f['nm'], str(e)))

if __name__ == '__main__':
	'''CLI Test case:
python configparser.py \
-w 127.0.0.1,8001,IPv4/UDP,default,puretcp,PureTCPProto \
-w 127.0.0.1,8002,IPv4/UDP,default,pureudp,PureUDPProto \
-o cyclon,Cyclon,cacheSize:40 -n alderan@inf.ufrgs.br
'''

	'''Configuration file test case:
<?xml version="1.0" ?>
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
	a = ConfigParser()
	b = ConfigParser()

	ConfigParser().parseOptions()

	print ConfigParser()['node']['name'], ConfigParser()['node']['domain']
