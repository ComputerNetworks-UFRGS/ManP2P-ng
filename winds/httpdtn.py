# -*- coding: utf-8 -*-

from twisted.enterprise import adbapi

from sqlite3 import Row

class Table(object):
	modified = None
	cursor = None

	schema = None

	joindata = None
	fielddata = None

	def __init__(self, *args, **kwargs):
		self.modified = set()
		self.attributes = [('id', None, int)] + self.attributes

		self.__load(**kwargs)

		if 'id' in kwargs:
			self.id = int(kwargs['id'])
			self.modified = set()

	def __load(self, *args, **kwargs):
		for attr, dflt, nrmlzr in self.attributes:
			if attr in kwargs and kwargs[attr] != None:
				setattr(self, attr, nrmlzr(kwargs[attr]))
			else:
				setattr(self, attr, dflt)

		return self

	def load(self, *args, **kwargs):
		self.__load(*args, **kwargs).modified = set()

		return self

	def __setattr__(self, attr, value):
		super(Table, self).__setattr__(attr, value)
		self.modified.add(attr)

	def getTable(self):
		r = '''"%s"''' % (self.tablename)

		if self.schema is not None:
			r = '''"%s".%s''' % (self.schema, r)

		return r

	def getChanges(self):
		return filter(lambda a: a[0] in self.modified, self.attributes)

	def insert(self, ignore=False):
		changes = self.getChanges()
		attrbts = None

		if len(changes) != 0 and len(changes) != len(self.attributes):
			attrbts = '(' + ','.join([a[0] for a in changes]) + ')'

		asksgn = ','.join(['?' for i in changes ])
		values = [getattr(self, a[0]) for a in changes]

		stmt = '''INSERT %s INTO %s%s VALUES (%s);''' % (
				'OR IGNORE' if ignore else '', self.getTable(),
				attrbts if attrbts is not None else '', asksgn)

		return stmt, values

	def update(self):
		changes = self.getChanges()

		asksgn = ','.join([a[0] + ' = ?' for a in changes])
		values = [ getattr(self, a[0]) for a in changes ]

		stmt = '''UPDATE %s SET %s WHERE id = %s;''' % (
									self.getTable(), asksgn, self.id)

		return stmt, values

	def delete(self, *args, **kwargs):
		attrbts = filter(lambda a: a[0] in kwargs, self.attributes)
		values = map(lambda t: t[2](kwargs[t[0]]), attrbts)

		assert len(attrbts) is not 0, "At least one criteria is needed"

		stmt = '''DELETE FROM %s WHERE %s''' % (self.getTable(),
							' AND '.join([a[0] + ' = ?' for a in attrbts]))

		return stmt, values

	def persist(self):
		stmt = None

		if self.id is None:
			return self.insert()

		return self.update()

	def select(self, *args, **kwargs):
		if self.joindata != None:
			fulltable = "%s %s" % (self.getTable(), self.joindata)
		else:
			fulltable = self.getTable()

		if self.fielddata != None:
			fields = self.fielddata
		else:
			fields = '*'

		stmt = '''SELECT %s FROM %s WHERE %%s;''' % (fields, fulltable)

		if 'raw' in kwargs and kwargs['raw'] == True:
			return stmt % kwargs['conditions'], [ ]

		attrbts = filter(lambda a: a[0] in kwargs, self.attributes)

		if len(attrbts) is not 0:
			asksgn = ' AND '.join(
				['%s = ?' % (a[0])
					if not isinstance(kwargs[a[0]], tuple)
					else '%s %s ?' % (a[0],kwargs[a[0]][0]) for a in attrbts])
		else:
			asksgn = '1 = 1'

		if 'offset' in kwargs:
			asksgn += ' LIMIT 1 OFFSET %s' % (kwargs['offset'])

		return stmt % asksgn, [kwargs[a[0]]
								if not isinstance(kwargs[a[0]], tuple)
								else kwargs[a[0]][1] for a in attrbts]

	def join(self, table, mine='', their='', kind='LEFT'):
		self.joindata = '%s JOIN %s ON (%s = %s)' % (kind, table.getTable(),
							"%s.%s" % (self.tablename, mine),
							"%s.%s" % (table.tablename, their))

		for f in table.attributes:
			if f not in self.attributes:
				self.attributes.append(f)

		return self

	def fields(self, fieldlist):
		self.fielddata = ', '.join(fieldlist)

		return self

class Package(Table):
	tablename = 'packages'

	attributes = [
		('name', '', str), ('md5', '', str), ('size', 0, int) ]

class Header(Table):
	tablename = 'headers'

	attributes = [
		('source', '', str), ('destination', '', str), ('package_id', None, int),
		('length', 0, int), ('date', '', str), ('md5', '', str) ]

class Body(Table):
	tablename = 'bodies'

	attributes = [ ('header_id', 0, int), ('begin', 0, int),
						('end', 0, int), ('content', '', str) ]

class Delivery(Table):
	tablename = 'deliveries'

	attributes = [ ('header_id', 0, int) ]

class TableFactory():
	def __init__(self, trnsctnmngr, table, joins=[]):
		self.trnsctnmngr = trnsctnmngr
		self.table = table
		self.joins = joins

	def execute(self, q, a):
		for r in self.trnsctnmngr.execute(q, a).fetchall():
			data = self.table(r)

			for t in self.joins:
				data.join(t)

			yield data.load(**r)

class DictRowledCPool(adbapi.ConnectionPool):
	def namedTuple(self, cursor, row):
		fields = { }

		for i, col in enumerate(cursor.description):
			if isinstance(row[i], str) or isinstance(row[i], unicode):
				fields[col[0].encode('utf-8')] = row[i].encode('utf-8')
			else:
				fields[col[0].encode('utf-8')] = row[i]

		return fields

	def connect(self):
		conn = adbapi.ConnectionPool.connect(self)
		conn.row_factory = self.namedTuple

		return conn

from twisted.internet.defer import Deferred, setDebugging
from twisted.internet.protocol import Protocol
from twisted.internet.address import IPv4Address

from twisted.python.failure import Failure

from twisted.web.client import Agent, FileBodyProducer
from twisted.web.http_headers import Headers
from twisted.web.resource import Resource
from twisted.web.server import Site, NOT_DONE_YET

from wind import AbstractWind, AbstractTCPWind, TCPWindFactory

from datetime import datetime

from StringIO import StringIO

setDebugging(True)

datamodel = '''-- HTTP-DTN Data model

PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS packages (
	id INTEGER PRIMARY KEY AUTOINCREMENT,
	name VARCHAR(160),
	md5 VARCHAR(32),
	size INTEGER,

	UNIQUE (name,md5)
);

CREATE UNIQUE INDEX IF NOT EXISTS packgesIDIndex ON packages(id);

CREATE TABLE IF NOT EXISTS headers (
	id INTEGER PRIMARY KEY AUTOINCREMENT,

	source VARCHAR(128),
	destination VARCHAR(128),

	package_id INTEGER REFERENCES packages(id) ON DELETE SET NULL ON UPDATE CASCADE,

	length INTEGER,
	date TEXT,

	md5 VARCHAR(32),

	UNIQUE (source,destination,date)
);

CREATE UNIQUE INDEX IF NOT EXISTS headersIDIndex ON headers(id);

CREATE TABLE IF NOT EXISTS bodies (
	id INTEGER PRIMARY KEY AUTOINCREMENT,

	header_id INTEGER REFERENCES headers(id),

	begin INTEGER,
	end INTEGER,

	content BLOB
);

CREATE UNIQUE INDEX IF NOT EXISTS bodiesIDIndex ON bodies(id);

CREATE TABLE IF NOT EXISTS deliveries (
	id INTEGER PRIMARY KEY AUTOINCREMENT,
	header_id INTEGER REFERENCES headers(id),

	UNIQUE (header_id)
);

CREATE UNIQUE INDEX IF NOT EXISTS deliveriesIDIndex ON deliveries(id);

'''

def headerExtractor(header, normalizers=[]):
	r = { }

	for t in header.getAllRawHeaders():
		label = t[0].lower().replace('content-', '').replace('http-dtn-', '')
		r[label] = t[1][0]

	for n in normalizers:
		if n[0] in r:
			r[n[0]] = n[2](r[n[0]])

	return r

TO_STRING_FMT = "%a, %d %b %Y %H:%M:%S GMT"
FM_STRING_FMT = "%a, %d %b %Y %H:%M:%S %Z"

def currentTimestring(self):
	return datetime.utcnow().strftime(TO_STRING_FMT)

def datetimeToTimestring(date):
	return date.strftime(TO_STRING_FMT)

def timestringToDatetime(date):
	return datetime.strptime(date, FM_STRING_FMT)

def datetimeToTimestamp(date):
	return int(date.strftime("%s"))

def timestampToDatetime(date):
	return datetime.utcfromtimestamp(date)

class HTTPDtnBodyConsumer(Protocol):
	def __init__(self, rh, header):
		self.header_id = header['id']

		if 'range' in rh:
			b,e = map(int, rh['range'].split(' to '))

			if (e - b) != rh['length']:
				rh['begin'], rh['end'] = b, e
			else:
				rh['begin'], rh['end'] = 0, rh['length']

		else:
			rh['begin'], rh['end'] = 0, rh['length']

		rh['content'] = ''

		self.rh = rh

	def dataReceived(self, bytes):
		self.rh['content'] += bytes

	def connectionLost(self, reason):
		b = Body(**self.rh)
		b.header_id = self.header_id

		def okprinter(_, *args, **kwargs):
			if isinstance(_, Failure):
				_.printTraceback()
			# ' -> Body persisted'
			return _

		HTTPDtnPersister.persistencer.runQuery(
							*b.insert(ignore=True)).addBoth(okprinter)

class RequestWrapper(object):
	def __init__(self, request):
		self.headers = request.requestHeaders
		self.content = request.content.read()

		self.length = len(self.content)

	def deliverBody(self, consumer):
		consumer.dataReceived(self.content)
		consumer.connectionLost(None)

class HTTPDtnPersister(object):
	persistencer = None

	def __init__(self, *args, **kwargs):
		HTTPDtnPersister.persistencer = DictRowledCPool(
						'sqlite3', 'test.db', check_same_thread=False)

		HTTPDtnPersister.persistencer.runInteraction(
										lambda r: r.executescript(datamodel))

	def getPersistencer(self):
		return HTTPDtnPersister.persistencer

	def returnObject(self, trnsctnmngr, (query, params), request):
		f = TableFactory(trnsctnmngr, Body, [Header()])

		for o in f.execute(query, params):
			request.setHeader('content-source', o.source)
			request.setHeader('content-destination', o.destination)
			request.setHeader('content-md5', o.md5)
			request.setHeader('date', o.date)
			request.setHeader('content-length', str(o.length))

			if o.begin != 0 or o.end != o.length:
				request.setHeader(
							'content-range', '%s to %s' % (o.begin, o.end))

			request.write(o.content)
			break

		else:
			request.setResponseCode(404)

		request.finish()

	def returnDelivery(self, trnsctnmngr, (query, params), request):
		f = TableFactory(trnsctnmngr, Delivery, [Header()])

		for d in f.execute(query, params):
			request.setHeader('content-source', d.source)
			request.setHeader('content-destination', d.destination)
			request.setHeader('content-md5', d.md5)
			request.setHeader('date', d.date)

			break

		else:
			request.setResponseCode(404)

		request.finish()

	def processBodies(self, trnsctnmngr, response, returnPath):
		h = headerExtractor(response.headers)
		h['length'] = response.length

		header = trnsctnmngr.execute(*Header().select(**h)).fetchone()

		if header is None: # ' -> Header not found on our headers table'
			trnsctnmngr.execute(*Header(**h).insert()).fetchone()
			header = trnsctnmngr.execute(*Header().select(**h)).fetchone()

		else: # ' -> Header already in DB'
			alreadyDelivered = trnsctnmngr.execute(
					*Delivery().select(header_id=header['id'])).fetchone()

			if alreadyDelivered is not None: # ' -> Header already delivered'
				# We don't know how to tell it to the guy; then we just return
				if returnPath is None:
					return True

				# Or, if we know the path, let's notify him
				httpHeader = Headers({
					'Content-source': [header['source'].encode('utf-8')],
					'Content-destination':
									[header['destination'].encode('utf-8')],
					'Content-md5': [header['md5'].encode('utf-8')],
					'Date': [header['date'].encode('utf-8')],
				})

				HTTPDTNWindClient(reactor).request(
					'POST', returnPath + 'delivery', httpHeader
				).addCallback(
					lambda r: True)

				return True

		# ' -> Persisting body'
		response.deliverBody(HTTPDtnBodyConsumer(h, header))

		return True

	def processDelivery(self, trnsctnmngr, response):
		h = headerExtractor(response.headers)
		header = trnsctnmngr.execute(*Header().select(**h)).fetchone()

		if header is None: # ' -> Headers not found on our headers table'
			trnsctnmngr.execute(*Header(**h).insert()).fetchone()
			header = trnsctnmngr.execute(*Header().select(**h)).fetchone()

			trnsctnmngr.execute(
					*Delivery(header_id=header['id']).insert(True)
			).fetchone()

		else: # ' -> Header already in our DB'
			trnsctnmngr.execute(
					*Delivery(header_id=header['id']).insert(True)
			).fetchone()

			trnsctnmngr.execute(
					*Body().delete(header_id=header['id'])).fetchone()

class HTTPDTNWindServer(Resource, AbstractWind, HTTPDtnPersister):
	isLeaf = True

	mandatoryHeadersFields = [
		'source', 'destination', 'md5', 'length', 'date' ]

	def __init__(self, *args, **kwargs):
		Resource.__init__(self, *args, **kwargs)
		HTTPDtnPersister.__init__(self)

	def connectionMade(self):
		Resource.connectionMade(self)
		AbstractWind.connectionMade(self)

	def render_GET(self, request):
		queryData = headerExtractor(request.requestHeaders)

		if request.path in ['/', '*']:
			fields = [
				'headers.' + n for n in [
					'source', 'destination', 'length', 'date', 'md5']] + [
				'bodies.' + n for n in ['begin', 'end', 'content']]

			query = Body().join(
				Header(), 'header_id', 'id'
			).fields(
				fields
			).select(**queryData)

			self.getPersistencer().runInteraction(
										self.returnObject, query, request)

		elif request.path in [ '/deliveries' ]:
			fields = ['headers.' + n for n in [
							'source', 'destination', 'length', 'date', 'md5']]

			query = Delivery().join(
						Header(), 'header_id', 'id').select(**queryData)

			self.getPersistencer().runInteraction(
									self.returnDelivery, query, request)

		else:
			request.setResponseCode(400)
			return ''

		return NOT_DONE_YET

	def chainBodies(self, *args, **kwargs):
		print ' -> Chain bodies vault'
		return ''

	def render_POST(self, request):
		senderHeader = headerExtractor(request.requestHeaders)

		#for f in self.mandatoryHeadersFields:
			#if f not in senderHeader:
				#request.setResponseCode(400)
				#return ''

		if request.path in ['/', '*']:
			returnPath = (
				'http://%(return-path)s:%(return-port)s/' % (senderHeader)
			) if ('return-path' in senderHeader
						and 'return-port' in senderHeader) else None

			self.getPersistencer().runInteraction(
					self.processBodies, RequestWrapper(request), returnPath)

		elif request.path in ['/delivery']:
			# The peer is trying to send us a delivery notification
			self.getPersistencer().runInteraction(
								self.processDelivery, RequestWrapper(request))

		else: # It doesn't know what he is doing... Is it really a peer?
			request.setResponseCode(400)

		return ''

class HTTPDTNWindClient(Agent, AbstractTCPWind, HTTPDtnPersister):
	connected = False

	''' -+- '''
	def __init__(self, *args, **kwargs):
		Agent.__init__(self, *args, **kwargs)

	def chainBodies(self, response, baseURL, offset=0):
		if response != None:
			if response.code != 200:
				return True

			self.getPersistencer().runInteraction(
										self.processBodies, response, baseURL)

		self.request(
			'GET', baseURL, Headers({'HTTP-DTN-Offset': [ offset ]})
		).addCallback(
			self.chainBodies, baseURL=baseURL, offset=offset+1)

		return True

	def chainDeliveries(self, response, baseURL, offset=0):
		if response != None:
			if response.code != 200:
				self.chainBodies(None, baseURL)
				return True

			self.getPersistencer().runInteraction(
										self.processDelivery, response)

		self.request(
			'GET', baseURL + 'deliveries',
			Headers({'HTTP-DTN-Offset': [ offset ]})
		).addCallback(
			self.chainDeliveries, baseURL=baseURL, offset=offset+1)

		return True

	def chainRequests(self, host, port):
		self.chainDeliveries(None, 'http://%s:%s/' % (host, port))

class HTTPDTNWind(AbstractWind):
	raw = True
	factory = None

	maxAge = 300

	def __init__(self, host, port, reactor):
		self.host, self.port = host, port
		self.last, self.line = datetime.now(), False
		self.reactor = reactor

	def __eq__(self, o):
		return self.port == o.port and self.host == o.host

	def howLong(self):
		return (datetime.now() - self.last).total_seconds()

	def worth(self):
		return (self.howLong() < HTTPDTNPeer.maxAge)

	def setFactory(self, f):
		self.factory = f

		return self

	def getPeer(self):
		return IPv4Address('TCP', self.host, self.port)

	def setLine(self, value):
		self.last = datetime.now()
		self.line = value

	def setOnline(self, callbacked):
		self.setLine(True)
		return callbacked

	def setOffline(self, callbacked):
		self.setLine(False)
		return callbacked

	def online(self):
		return self.line

	def spread(self, headers, data):
		for p in self.factory.knownHosts:
			baseURL = 'http://%s:%s/' % (p.host, p.port)

			# TODO: Spread the word
			HTTPDTNWindClient(
				self.reactor
			).request(
				'PUT', baseURL, headers, FileBodyProducer(StringIO(data))
			).addCallbacks(
				callback=p.setOnline,
				errback=p.setOffline
			).addErrback(
				lambda r: True)

	def doSend(self, message, data):
		cmd5 = md5(data).hexdigest()
		date = currentTimestring()

		baseURL = 'http://%s:%s/' % (self.host, self.port)

		headers = Headers({
			'content-source': message.getSource(),
			'content-destination': message.getDestination(),
			'content-md5': cmd5, 'content-length': len(data),
			'date': date,
		})

		if self.online() is True or self.worth():
			return HTTPDTNWindClient(
				self.reactor
			).request(
				'PUT', baseURL, headers, FileBodyProducer(StringIO(data))
			).addCallbacks(
				callback=self.setOnline,
				errback=self.setOffline,
			).addErrback(
				self.spread, headers, data)

		return self.spread(headers, data)

class HTTPDTNWindFactory(Site, TCPWindFactory):
	knownHosts = set()

	def __init__(self, overlay, rcr=None):
		Site.__init__(self, HTTPDTNWindServer())
		TCPWindFactory.__init__(self, overlay, rcr)

	def listen(self, port, address=None):
		interface = '' if address is None else address
		# TODO: add the address parameter to the listener
		return Site.listen(self, port)

	def doConnect(self, (host, port)):
		t = HTTPDTNWind(host, port, self.reactor)
		t.setFactory(self).connectionMade()

		self.knownHosts.add(t)













if __name__ == '__main__':

	from twisted.internet import reactor
	from sys import argv

	shallServ = False

	if 'client' in argv:
		shallServ = True
		reactor.callLater(
			1, HTTPDTNWindClient(reactor).chainRequests, '143.54.12.57', 8080)

	if 'server' in argv or shallServ:
		SiteFactory(None).listen(8080)

	if 'sql' in argv:
		p = Package(name='asdsa', md5='asdsa')
		p.name = 'pedrotestes'

		q = Package(name='dsads', md5='asdsa', size=20)

		print p.name, p.md5, p.size, p.modified, p.persist()
		print q.name, q.md5, q.size, q.modified, q.persist()

		print Package().select(name=10, size=('>=', 100))
		print Package().select(raw=True, conditions='ASD = DSA')

		print Delivery().join(
			Header(), 'header_id', 'id'
		).fields([
			'header.source', 'header.destination'
		]).select(
			source='alderan', destination='dagobah')

	reactor.run() if reduce(lambda a,b: a or b,
						filter(lambda c: c in argv,
								['server','client']), False) else None

