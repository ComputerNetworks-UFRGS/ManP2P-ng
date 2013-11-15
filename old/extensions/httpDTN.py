# -*- coding: utf-8 -*-
# # # # # # # # # # # # # # # # # # ## # # # # # # # # # # # # # # # # # # # ##

from extensionLoader import ExtensionLoader
from overlay import addHaltFunction as aHF
from commonRegex import HostName
from commandLineParser import CommandLineParser

from twisted.enterprise import adbapi
from twisted.internet import reactor
from twisted.python.failure import Failure

from hashlib import md5
from sys import stderr
from sqlite3 import Row

import time

import re

dtnGroup = None
bundles = None

#
# Auxiliary class
#

class DictRowledCPool(adbapi.ConnectionPool):
	def connect(self):
		conn = adbapi.ConnectionPool.connect(self)
		conn.row_factory = Row
		return conn

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
#
# Auxiliary functions and variables.
#	Used all along the code in a DRY style
#
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

dateFormat = '%a, %d %b %Y %H:%M:%S GMT'

def formatedTime(date=None):
	if date is None:
		return time.strftime(dateFormat, time.gmtime())
	else:
		return time.strftime(dateFormat, time.gmtime(date))

def rawTime(date=None):
	if date is None:
		return 0

	return int(time.mktime(time.strptime(date, dateFormat)))

def processHeaders(header):
	h = { }

	# Separate the header between the request, the fields  and the data.
	fieldsStart = header.find('\n') + 1
	contentStart = header.find('\n\n') + 2

	# The resource requested
	request = header[:fieldsStart].split()[1]

	# Parse the fields and add them to the dictionary 'h'
	for l in header[fieldsStart:contentStart].strip().split('\n'):
		h[l[:l.find(':')]] = l[l.find(':')+1:].strip()

	return h, request, header[contentStart:]

bundleBody = '''Host: {host}
Content-Destination: {contentDestination}
Content-Source: {contentSource}
Content-Length: {contentLength}
Content-Range: bytes {contentRange}/{contentRangeEnd}
Content-MD5: {contentMD5}
Date: {date}

{content}'''

def formatBundleBody(body, r, h):
	return body.format(
		contentSource = r[0],
		contentDestination = r[1],
		contentMD5 = r[2],
		contentLength = r[3],
		contentRange = r[4],
		contentRangeEnd = r[4] + r[3],
		host = h,
		date = formatedTime(r[5]),
		content = r[6],
	)

nok = '''http-dtn:HTTP/1.1 404 Not Found
Date: {date}

'''

## # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
#
# Code related to GET requests
#
## # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

getBundleOk = 'http-dtn:HTTP/1.1 200 OK\n' + bundleBody
getNotFound = nok

def getBundles(txn, sParser):
	# Auxiliary list for recording which of our records were delivered to its
	# destination.
	deliveredBundles = [ ]

	# For every bundle we got with our request, lets issue a 200 OK message for
	# the requester
	for r in txn.execute('SELECT * FROM bundle').fetchall():
		sParser.write(formatBundleBody(getBundleOk, r, sParser.myNick))

		# Now, we are checking if we have delivered the bundle to its
		# destination. If yes, lets put it in a list for removing it from
		# our database and flag it as delivered.
		if (sParser.remotePeerNick is not None
			and sParser.remotePeerNick == r[1]):
				deliveredBundles.append(r)

	# If we have no bundles or if we have already sent the requester peer all
	# the bundles we have, lets signal it to him through a 404 Not Found so it
	# can get knowledge about it
	sParser.write(getNotFound.format(date = formatedTime()))

	# Remember the list of delivered bundles? Ok, now we'll remove the delivered
	# ones from the 'bundle' table and put them on the 'delivered' table. No need
	# to do more evaluation on the content of the list as it will be empty if we
	# are not connected to the other peer.
	for r in deliveredBundles:
		txn.execute(
			'INSERT INTO delivered values (?, ?, ?, ?);',
			(r[0], r[1], r[2], int(time.time())))

		txn.execute(
			'DELETE FROM bundle WHERE source = ? AND \
			destination = ? and md5 = ?;',
			(r[0], r[1], r[2]))

getDeliveriesOk = '''http-dtn:HTTP/1.1 200 Ok
Date: {date}
Content-Source: {contentSource}
Content-Destination: {contentDestination}
Content-MD5: {contentMD5}

'''

def getDeliveries(txt, sParser):
	# Lets say him all bundles we have delivered to their destinations
	for r in txt.execute('SELECT * FROM delivered;').fetchall():
		sParser.write(
			getDeliveriesOk.format(
				contentSource = r[0],
				contentDestination = r[1],
				contentMD5 = r[2],
				date = r[3],))

	# If we have not delivered nothing or if we already said him about all of
	# the bundles we have delivered, lets issue a 404 Not Found.
	sParser.write(getNotFound.format(date=formatedTime()))

getMustFields = [
	'Date',
	'Host',
]

def processGET(sParser, header):
	fields, request, content = processHeaders(header)

	# Searching for RFC MUST fields If they are not present, lets return a error
	# for the requester. But not now :-)
	for f in getMustFields:
		if f not in fields:
			pass

	# Let's check what out requester have requested.
	# Did he requested all bundles?
	if request == '*':
		bundles.runInteraction(
			getBundles, sParser
		).addBoth(
			lambda r: None)

	# Or did he requested what we had delivered in our random walks?
	elif request == 'deliveries':
		bundles.runInteraction(
			getDeliveries, sParser
		).addCallback(
			getDeliveries, sParser)

	# OMG, he requested something never thought before! Lets pass :-)
	else:
		pass

## # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
#
# Code related to PUT requests
#
## # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

putOk = '''http-dtn:HTTP/1.1 200 OK
Date: {date}

'''

putNok = nok

def putCallback(result, sParser=None):
	# We'll check if we have successfully added the bundle to our database in
	# which case we have a type(result) equal list, or, if we already get this
	# bundle in our database, in which case we have a type(result) equal to
	# Failure. In case we are sending a error message, result is None. Elsewhere,
	# we have a unexpected message, so we stay shut and just ignore the dammed
	# message :-)
	if isinstance(result, list) == True:
		m = putOk.format(date=formatedTime())

	elif isinstance(result, Failure) == True:
		m = putNok.format(date=formatedTime())

	elif result is None:
		m = putNok.format(date=formatedTime())

	else:
		m = None

	if m is not None:
		sParser.write(m)

	return True


ftokens = { }

def addToken(token, callback):
	if token in ftokens:
		raise RuntimeError, "Token %s already in set" % (token)

	assert callable(callback), "The callback is not callable"

	ftokens[token] = callback

putMustFields = [
	('Host', lambda s: HostName.match(s)),
	('Content-Source', lambda s: HostName.match(s)),
	('Content-Destination', lambda s: HostName.match(s) or s == '*'),
	('Content-Length', lambda s: re.compile('[0-9]+').match(s)),
	('Content-MD5', lambda s: re.compile('^[0-9a-z]{32}$').match(s)),
	('Date', lambda s: True),
]

def processPUT(txn, sParser, header):
	fields, request, content = processHeaders(header)

	# Lets check the fields we have with those who are said as a MUST in our RFC
	for f in putMustFields:
		# If they are not present, we must raise a error. But not now
		if f[0] not in fields:
			return putCallback(None, sParser)

		# Now, lets see if they are who they say they are. If they aren't, we
		# shall raise a error but not now. For the while, we'll just drop the
		# bundle by returning
		if f[1](fields[f[0]]) is None:
			return putCallback(None, sParser)

	# We now should convert the date passed through Date: to a format that may
	# be stored in our database, which is INTERGER
	try:
		fields['Date'] = rawTime(fields['date'])

	# If we can't do the conversion, we can't store this bundle. Then, we shall
	# return a error to the requester, but I don't know which of the error
	# codes. We must discuss it.
	except ValueError:
		return putCallback(None, sParser)

	# If we have a bundle in the 'delivered' table, it means that we have already
	# delivered it to the destination
	query = '''SELECT * FROM delivered WHERE
					source = ? AND destination = ? AND md5 = ?'''
	t = (
		fields['Content-Source'],
		fields['Content-Destination'],
		fields['Content-MD5'],
	)

	# Checking if the bundle is in the 'delivered' table
	if len(txn.execute(query, t).fetchall()) != 0:
		return putCallback(None, sParser)

	# Now it is time to store the bundle in our database. So, lets do it through
	# a Deffer. However, if we are the destination of the bundle, lets process it
	# content... How? I don't know yet :-(
	if fields['Content-Destination'] not in [sParser.myNick, '*']:
		bundles.runQuery(
			'INSERT INTO bundle VALUES (?, ?, ?, ?, ?, ?, ?)', (
				fields['Content-Source'],
				fields['Content-Destination'],
				fields['Content-MD5'],
				fields['Content-Length'],
				0,
				fields['Date'],
				content,)
		).addBoth(
			putCallback, sParser=sParser)

	else:
		bundles.runQuery(
			'INSERT INTO delivered VALUES (?, ?, ?, ?)', (
				fields['Content-Source'],
				fields['Content-Destination'],
				fields['Content-MD5'],
				formatedTime(),)
		).addBoth(
			putCallback, sParser=sParser)

		#
		# So, a module must be environment aware. In other words, a module must
		# be implemented considering a DTN and register it self with us.
		try:
			ftokens[
				content[:content.find(':')]
			](fields, content[:content.find(':')])

		except KeyError:
			print "Unknow token for DTN message <%s %s %s>" % (
				fields['Content-Source'],
				fields['Content-Destination'],
				fields['Content-MD5']
			)

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
#
# Processing HTTP codes (like 200, 404, among other... or not)
#
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

def processDelivery(txn, sParser, data):
	fields, nouse, content = processHeaders(data)

	whatMustHave = (
		fields['Content-Source'],
		fields['Content-Destination'],
		fields['Content-MD5']
	)

	iStatement = 'INSERT INTO delivered VALUES (?, ?, ?, ?)'
	dStatement = 'DELETE FROM bundle WHERE md5 = ?'
	sStatement ='''SELECT md5 FROM bundle
							WHERE
							source = ? AND destination = ? AND md5 = ?'''

	txn.execute(
		dStatement, whatMustHave
	).fetchall() if (
		len (txn.execute(sStatement, whatMustHave).fetchall()) > 0
	) else None

	txn.execute(iStatement, whatMustHave + (formatedTime(),)).fetchall()

def processHTTPCode(txn, sParser, data):
	fields, code, content = processHeaders(data)

	try:
		code = int(code)
	except ValueError as e:
		print >> stderr, 'HTTP return code not integer'
		return

	if code == 404:
		return

	elif code == 200:
		isGetResponse = reduce(
			lambda r,s: r and s,
			map(lambda k: k[0] in fields,
				putMustFields)
		)

		if isGetResponse:
			processPUT(txn, None, data)
			return

		isDeliveryResponse = reduce(
			lambda r,s: r and s,
			map(lambda k: k in fields,
				['Content-Source', 'Content-Destination', 'Content-MD5'])
		)

		if isDeliveryResponse:
			processDelivery(txn, None, data)
			return

	'''Nothing to do this part on'''

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
#
# Handshaking
#
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
put = 'http-dtn:PUT * HTTP/1.1\n' + bundleBody
get = '''http-dtn:GET {thing} HTTP/1.1
Host: {host}
Date: {date}

'''

def putRequest(r, h):
	return formatBundleBody(put, r, h)

def dtnHandshake(txn, sParser):
	qDelivering = 'SELECT * FROM bundle WHERE destination = ? ORDER BY date;'

	for r in txn.execute(qDelivering, (sParser.remotePeerNick,)).fetchall():
		sParser.write(putRequest(r, sParser.myNick))

		txn.execute(
			'INSERT INTO delivered values (?, ?, ?, ?);',
			(r[0], r[1], r[2], int(time.time()))
		)

		txn.execute(
			'DELETE FROM bundle WHERE source = ? AND \
			destination = ? and md5 = ?;',
			(r[0], r[1], r[2])
		)

	# Ask for delivered bundles
	sParser.write(get.format(
		thing='deliveries', host=sParser.myNick, date=formatedTime()
	))

	# Ask for other bundles
	sParser.write(get.format(
		thing='*',host=sParser.myNick, date=formatedTime()
	))

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
#
# Talking to the overlay
#
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

def dtnParser(sParser, t):
	m = t[1][:t[1].find(' ')]

	if m == "GET":
		processGET(sParser, t[1])

	elif m == "PUT":
		bundles.runInteraction(
			processPUT, sParser, t[1]
		).addBoth(
			lambda r: True)

	elif m == "HTTP/1.1":
		bundles.runInteraction(
			processHTTPCode, sParser, t[1]
		).addBoth(
			lambda r: True)

	else:
		'''WTF?'''
		pass

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
#
# Auxiliar for adding and spreading bundles
#
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
class BundleWriter(dict):
	def __init__(self, prepend=''):
		self.prepend = prepend

	def write(self, destination, message):
		self.message = message = {
			'contentSource': sParser.myNick,
			'contentDestination': destination,
			'contentMD5': md5(message).hexdigest(),
			'contentLength': len(message),
			'date': formatedTime(),
			'content': self.prepend + message,
			# Not for storing
			'host': sParser.myNick,
			'contentRange': 0,
			'contentRangeEnd': len(message)
		}

		return bundles.runQuery (
			'INSERT INTO bundle VALUES (?, ?, ?, ?, ?, ?, ?)', (
				message['contentSource'],
				message['contentDestination'],
				message['contentMD5'],
				message['contentLength'],
				0,
				message['date'],
				content,
			)
		).addCallback(self.spread)

	def spread(self, result):
		if not isinstance(result, list):
			print '''For some reason we can't add the bundle into our DB'''
			return

		for r in result:
			for p in dtnGroup:
				p.transport.write(putRequest(r, r[0]))

		return result

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
#
# Bureaucratic code
#
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

def extensionName():
	return str("HTTP-DTN")

def extendProtocol(lFactory, sFactory):
	global dtnGroup, bundles

	if not ExtensionLoader().isActive("Groups"):
		return False

	if not ExtensionLoader().isActive("BasicOverlay"):
		return False

	try:
		dtnGroup = (ExtensionLoader().
						getExtension("Groups").
							getGroup("HTTP-DTN"))
	except KeyError:
		return False

	bundles = DictRowledCPool(
		"sqlite3",
		CommandLineParser().getArguments().PeerName + '-http-dtn.db',
		check_same_thread=False
	)

	bundles.runInteraction(
		lambda r: r.executescript('''
            -- 0       1            2    3       4      5     6
            -- source, destination, md5, length, range, date, content
            CREATE TABLE IF NOT EXISTS bundle (
                source VARCHAR(32),
                destination VARCHAR(32),
                md5 VARCHAR(32),
                length INTERGER,
                range INTERGER CHECK (range >= 0),
                date INTERGER CHECK (date > 0),
                content BLOB,

                PRIMARY KEY (source, destination, md5)
            );

            CREATE TABLE IF NOT EXISTS delivered (
                source VARCHAR(32),
                destination VARCHAR(32),
                md5 VARCHAR(32),
                date INTERGER CHECK (date > 0),

                PRIMARY KEY (source, destination, md5)
            );
	'''))

	dtnRE = (
		'http-dtn:(({0})|({1}))'.format(
			# Request headers
			'(GET|PUT) ([0-9]+|\*) HTTP/1.1\n' +
			'([-A-Za-z0-9]+:[^\n]+\n)+\n' +
			'.*', # The content

			# Response headers
			'HTTP/1.1 [0-9]+ [- a-zA-Z0-9]+\n' +
			'([-A-Za-z0-9]+: [^\n]+\n)+\n' +
			'.*',
		)
	)

	sFactory.addToken('http-dtn', dtnParser)

	(lFactory.
		getState('start').
			addTransition('start', dtnRE, lambda t: (t[:8], t[9:])))

	(lFactory.
		getState('established').
			addTransition('established', dtnRE, lambda t: (t[:8], t[9:])))

	(ExtensionLoader().
		getExtension("BasicOverlay").
			addBootstrapFunction(
				lambda sParser: bundles.runInteraction(dtnHandshake, sParser)))

	return True
