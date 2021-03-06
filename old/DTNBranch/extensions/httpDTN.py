# -*- coding: utf-8 -*-
## # # # # # # # # # # # # # # # # # ## # # # # # # # # # # # # # # # # # # # ##

from extensionLoader import ExtensionLoader
from overlay import addHaltFunction as aHF
from commonRegex import HostName
from commandLineParser import CommandLineParser

from twisted.enterprise import adbapi as dbase
from twisted.internet import reactor
from twisted.python.failure import Failure

import time

import re

dtnGroup = None
bundles = None

## # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
#
# Auxiliary functions and variables.
#	Used all along the code in a DRY style
#
## # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

dateFormat = '%a, %d %b %Y %H:%M:%S GMT'

def formatedTime(date=None):
	if date is None:
		return time.strftime(dateFormat, time.gmtime())
	else:
		return time.strftime(dateFormat, time.gmtime(date))

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
	# Auxiliary list
	l = [ ]

	# For every bundle we got with our request, lets issue a 200 OK message for
	# the requester
	for r in txn.execute('SELECT * FROM bundle').fetchall():
		#m = getBundleOk.format(
			#contentSource = r[0],
			#contentDestination = r[1],
			#contentMD5 = r[2],
			#contentLength = r[3],
			#contentRange = r[4],
			#contentRangeEnd = r[4] + r[3],
			#host = sParser.myNick,
			#date = formatedTime(r[5]),
			#content = r[6],
		#)

		#sParser.write(m)
		sParser.write(formatBundleBody(getBundleOk, r, sParser.myNick))

		# Now, we are checking if we have delivered the bundle to its
		# destination. If yes, lets put it in a list for removing it from
		# our database and flag it as delivered.
		if (sParser.remotePeerNick is not None
			and sParser.remotePeerNick == r[1]): l.append((r[0], r[1], r[2]))

	# If we have no bundles or if we have already sent the requester peer all
	# the bundles we have, lets signal it to him through a 404 Not Found so it
	# can get knowledge about it
	sParser.write(getNotFound.format(date = formatedTime()))

	# Remember the list of delivered bundles? Ok, now we'll remove the delivered
	# ones from the 'bundle' table and put them on the 'delivered' table. No need
	# to do more evaluation on the content of the list as it will be empty if we
	# are not connected to the other peer.
	for r in l:
		txn.execute(
			'INSERT INTO delivered values (?, ?, ?, ?);',
			(r[0], r[1], r[2], int(time.time()))
		)

		txn.execute(
			'DELETE FROM bundle WHERE source = ? AND \
			destination = ? and md5 = ?;',
			(r[0], r[1], r[2])
		)

getDeliveriesOk = '''http-dtn:HTTP/1.1 200 Ok
Date: {date}
Content-Source: {contentSource}
Content-Destination: {contentDestination}
Content-MD5: {contentMD5}

'''

def getDeliveries(result, sParser):
	# Lets say him all bundles we have delivered to their destinations
	for r in result:
		m = getDeliveriesOk.format(
			contentSource = r[0],
			contentDestination = r[1],
			contentMD5 = r[2],
			date = r[3],
		)

		sParser.write(m)

	# If we have not delivered nothing or if we already said him about all of
	# the bundles we have delivered, lets issue a 404 Not Found.
	else:
		m = getNotFound.format (date = formatedTime())
		sParser.write(m)

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
		bundles.runInteraction(getBundles, sParser).addBoth(lambda r: None)
	# Or did he requested what we had delivered in our random walks?
	elif request == 'deliveries':
		bundles.runQuery(
			'SELECT * FROM delivered;').addCallback(getDeliveries, sParser)
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
		m = putOk.format(date = formatedTime())

	elif isinstance(result, Failure) == True:
		m = putNok.format(date = formatedTime())

	elif result is None:
		m = putNok.format(date = formatedTime())

	else:
		m = None

	if m is not None:
		sParser.write(m)

	return True

putMustFields = [
	('Host', lambda s: HostName.match(s)),
	('Content-Source', lambda s: HostName.match(s)),
	('Content-Destination', lambda s: HostName.match(s)),
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
		fields['Date'] = int(
							time.mktime(
								time.strptime(
									fields['Date'], dateFormat)))

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
	if fields['Content-Destination'] != sParser.myNick:
		bundles.runQuery(
			'INSERT INTO bundle VALUES (?, ?, ?, ?, ?, ?, ?)',
			(
				fields['Content-Source'],
				fields['Content-Destination'],
				fields['Content-MD5'],
				fields['Content-Length'],
				0,
				fields['Date'],
				content,
			)
		).addBoth(putCallback, sParser=sParser)

	else:
		bundles.runQuery(
			'INSERT INTO delivered VALUES (?, ?, ?, ?)',
			(
				fields['Content-Source'],
				fields['Content-Destination'],
				fields['Content-MD5'],
				formatedTime(),
			)
		).addBoth(putCallback, sParser=sParser)

		# TODO: Process the content
		pass

## # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
#
# Talking to the overlay
#
## # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

def dtnParser(sParser, t):
	m = t[1][:t[1].find(' ')]

	if m == "GET":
		processGET(sParser, t[1])
	elif m == "PUT":
		bundles.runInteraction(processPUT, sParser, t[1])
	elif m == "HTTP/1.1":
		pass
	else:
		'''WTF?'''
		pass

put = 'http-dtn:PUT * HTTP/1.1\n' + bundleBody

def putRequest(r, h):
	return formatBundleBody(put, r, h)

def dtnHandshake(txn, sParser):
	query = 'SELECT * FROM bundle WHERE destination = ?'

	l = txn.execute(query, (sParser.remotePeerNick,)).fetchall()

	for r in l:
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

## # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
#
# Bureaucratic code
#
## # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

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

	bundles = dbase.ConnectionPool("sqlite3", CommandLineParser().getArguments().PeerName + '-http-dtn.db',
	check_same_thread = False)

	# 0       1            2    3       4      5     6
	# source, destination, md5, length, range, date, content
	bundles.runQuery('''
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
	''').addBoth(lambda result: result)

	bundles.runQuery('''
		CREATE TABLE IF NOT EXISTS delivered (
			source VARCHAR(32),
			destination VARCHAR(32),
			md5 VARCHAR(32),
			date INTERGER CHECK (date > 0),
			PRIMARY KEY (source, destination, md5)
		);
	''').addBoth(lambda result: result)

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
			addTransition('start', dtnRE, lambda t: (t[:8], t[9:]))
	)
	(lFactory.
		getState('established').
			addTransition('established', dtnRE, lambda t: (t[:8], t[9:]))
	)

	(ExtensionLoader().
		getExtension("BasicOverlay").
			addBootstrapFunction(
				lambda sParser: bundles.runInteraction(dtnHandshake, sParser)
			)
	)

	return True
