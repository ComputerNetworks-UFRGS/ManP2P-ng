# -*- coding: utf-8 -*-

from twisted.internet.task import LoopingCall

from extensionLoader import ExtensionLoader

from xml.dom.minidom import parseString, Document
from xml.parsers.expat import ExpatError

from tempfile import TemporaryFile
from imp import load_module as LoadModule, PY_SOURCE

from sys import stderr

from hashlib import md5

dtnMonitors = None
dtnExtension = None
BundleWriter = None

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# # # # # # # # # # # # # # # # Auxiliary Functions # # # # # # # # # # # # # #
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
hasNode = lambda dom,name: len(dom.getElementsByTagName(name)) > 0

def parsePlanMessage(data):
	t = (
		hasNode(data, 'nodes') and
		hasNode(data, 'plan') and
		data.hasAttribute('id')
	)

	assert t is not False, (
		"Missing elements in request")

	try:
		targets = [ dict (
				(str(k), str(v)) for k,v in n.attributes.items()
			) for n in
				data.getElementsByTagName('targets')[-1].childNodes ]

		plan = dict (
			(str(k), str(v)) for k,v in
				data.getElementsByTagName('plan')[-1].attributes.items() )

		assert 'plan' not in plan, (
		'An attribute named plan should not exist')

		plan['plan'] = (
			data.getElementsByTagName('plan')[-1].firstChild.wholeText)

	except IndexError:
		raise RuntimeError, "Expecting some data in plan"

	for i in ['plan', 'period']:
		assert i in plan, 'Attribute %s not in plan' % (i)

	assert md5(plan['plan']).hexdigest() == plan['checksum'], (
		'Computed MD5 does not match the one in the request')

	return (targets, plan)
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# # # # # # # # # # # # # End of Auxiliary Functions  # # # # # # # # # # # # #
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

def distributePlan(txn, targets, plan):
	m = Document()

	message = m.appendChild(m.createElement('message'))
	message.setAttribute('kind', 'distribution')

	targetNodes = message.appendChild(m.createElement('targets'))
	planNode = message.appendChild(m.createElement('plan'))

	for t in targets:
		newTarget = m.createElement('target')

		for k,v in t.itervalues():
			newTarget.setAttribute(k,v)

		targetNodes.appendChild(newTarget)

	for k,v in plan.itervalues():
		if k is 'plan':
			continue

		planNode.setAttribute(k,v)

	planNode.appendChild(m.createTextNode(plan['plan']))

	peers = txn.execute(
		'SELECT peer FROM monitoring_group ORDER BY peer'
	).fetchall()


	writer = BundleWriter('dtnmonitor:')
	for p in peers:
		writer.write(p['peer'], m.toxml())

	return targets, plan

def persistPlan(txn, targets, plan):
	txn.execute(
		'INSERT INTO monitoring_plans VALUES (?, ?)',
		(plan['checksum'], plan['plan']))

	for t in targets:
		txn.execute(
			'INSERT INTO monitoring_targets VALUES (?, ?, ?, ?)'
			(plan['md5'], t['address'], t['port'], t['transport'])
		)

		nottorecord = {
			'md5': None, 'address': None,
			'port': None, 'transport': None
		}

		for k,v in t.itervalues():
			txn.execute(
				'INSERT INTO monitoring_data VALUES (?, ?, ?, ?, ?, ?)',
				(	plan['md5'], t['address'],
					t['port'], t['transport'],
					str(k), str(v)
				)
			) if k not in nottorecord else None

	return targets, plan

monitoringPlans = { }

class AnomalyError(Exception):
	def __init__(self, anyRelevantData):
		self.detail = anyRelevantData

	def getDetails(self):
		return self.detail

def notifyHealers(txn, errorDetails, pid, tid, extraData):
	m = Document()

	notification =  m.appendChild(m.createElement('message'))
	notification.setAttribute('kind', 'fault notification')

	faultNode = m.createElement('faulty')

	for k,v in tid:
		faultNode.setAttribute(str(k), str(v))

	error = m.createElement('errors')

	for k,v in errorDetails:
		error.setAttribute(str(k), str(v))

	notification.appendChild(faultNode)
	notification.appendChild(error)

	'''We are sending the notification... But to whom??'''

	healers = txn.runQuery('SELECT * FROM healing_group')
	monitors = txn.runQuery('SELECT * FROM monitoring_group')

	writer = BundleWriter('dtnhealing:')

	for p in healers:
		writer.write(m.toxml())

	for p in monitors:
		writer.write(m.toxml())

	monitoringPlans[pid][tid].stop()

def executePlan(txn, pid, tid):
	extraData = dict(
		map(lambda r: (r['attribute'], r['data']),
			txn.execute('''
				SELECT attribute,data FROM monitoring_data
				WHERE md5 = ? address = ? AND port = ? AND transport = ?;''',
				((pid,) + tid))))

	try:
		LoadModule(
			'monitoring-dtn-plan-' + pid,
			monitoringPlans[pid]['tmp'],
			'monitoring-dtn-plan-' + pid,
			('py0', '', PY_SOURCE)
		).execute(
			tid, extraData
		).addCallbacks(
			callback=lambda m: True,
			# Errback and its data
			errback=lambda r: dtnExtension.bundles.runInteraction(
				notifyHealers, r, pid, tid, extraData)
		)

	except AnomalyError as e:
		dtnExtension.bundles.runInteraction(
			notifyHealers, e.getDetails(), pid, tid, extraData)

def startPlan(txn, targets, plan):
	if plan['md5'] not in monitoringPlans:
		monitoringPlans[plan['md5']] = { }

		monitoringPlans[plan['md5']]['tmp'] = TemporaryFile()
		monitoringPlans[plan['md5']]['tmp'].write(plan['plan'])

	for t in targets:
		ch = (t['address'], t['port'], t['transport'])

		assert ch not in monitoringPlans[plan['md5']], (
			'Monitoring plan %s for <%s, %s, %s> already in execution' %
			((plan['md5'],) + ch)
		)

		monitoringPlans[plan['md5']][ch] = LoopingCall(
			lambda: dtnExtension.bundles.runInteraction,
			executePlan, pid=plan['md5'], tid=ch
		).start(
			int(plan['period']), False)

	return True

def stopPlan(txn, message):
	try:
		faulty = dict (
			(str(k), str(v)) for k,v in
				data.getElementsByTagName('faulty')[-1].attributes.items() )

	except IndexError as e:
		return

	tid = (faulty['address'], faulty['port'], faulty['transport'])

	plan = txn.runQuery('''
		SELECT md5 FROM monitoring_targets WHERE
			address = ? AND port = ? AND transport = ?;''', tid).fetchone()

	if plan is None or plan['md5'] not in monitoringPlans:
		print >> stderr, '''Can't find a plan for <%s %s %s>''' % (tid)
		return

	monitoringPlans[plan['md5']][tid].stop()

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# Parsing message and managing groups
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

def DTNMonitoringParser(header, content):
	try:
		data = parseString(content).getElementsByTagName('message')[0]

	except ExpatError:
		print >> stderr, '*** Can not parse content ***\n' + content + '\n***'
		return False

	except IndexError:
		print >> stderr, '*** Unknown data in message ***'
		return False

	kind = data.getAttribute('kind')

	if kind == 'request':
		m = Document()

		n = m.appendChild(m.createElement('message'))
		n.setAttribute('kind', 'request reply')
		n.setAttribute('for', data.getAttribute('id'))

		try:
			dtnExtension.bundles.runInteraction(
				distributePlan, *parsePlanMessage(data)
			).addCallback(
				lambda r: dtnExtension.bundles.runInteraction(persistPlan, r)
			).addCallback(
				lambda r: dtnExtension.bundles.runInteraction(startPlan, r)
			).addBoth(
				lambda r: True)

			n.setAttribute('status', 'accepted')

		except AssertionError as e:
			print >> stderr, (
				'Rejecting a DTN monitoring request. Cause: %s' % (str(e)))

			n.setAttribute('status', 'rejected')

		finally:
			BundleWriter().write(header['Content-Source'], m.toxml())

	elif kind == 'distribution':
		dtnExtension.bundles.runInteraction(
			persistPlan, *parsePlanMessage(data)
		).addBoth(
			lambda r: True)

	elif kind == 'fault notification':
		dtnExtension.bundles.runInteraction(
			stopPlan, message)

	elif kind == 'join group':
		source == header['Content-Source']

		dtnExtension.bundle.runQuery(
			'INSERT INTO monitoring_group VALUES (?, ?);',
			(source, time())
		).addCallback(
			lambda r: True
		).addErrback(
			lambda r: dtnExtension.bundle.runQuery(
				'UPDATE monitoring_group SET join_date = ? WHERE peer = ?;',
				(time(), source)
			).addBoth(lambda r: True)
		).addBoth(
			lambda r: True)

	elif kind == 'part group':
		source == header['Content-Source']

		dtnExtension.bundle.runQuery(
			'DELETE FROM monitoring_group WHERE peer = ?',
			(source,)
		).addBoth(
			lambda r: True)

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# Bureaucratic stuff
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
extName = 'DTN-MONITOR'

def extensionName():
	return extName

def extendProtocol(lFactory, sFactory):
	global dtnMonitors, BundleWriter

	try:
		if ExtensionLoader().isActive("Groups") is False:
			raise KeyError, "Group extension not active"

		if ExtensionLoader().isActive("HTTP-DTN") is False:
			raise KeyError, "HTTP-DTN extension not active"

		dtnMonitors = (ExtensionLoader().
						getExtension("Groups").
							getGroup("DTN-Monitors"))

		dtnExtension = ExtensionLoader().getExtension("HTTP-DTN")
		BundleWriter = dtnExtension.BundleWriter

	except KeyError as e:
		return False

	dtnExtension.addToken('dtnmonitor', DTNMonitoringParser)

	return True
