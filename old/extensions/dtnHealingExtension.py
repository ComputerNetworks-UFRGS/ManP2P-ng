# -*- coding: utf-8 -*-

from extensionLoader import ExtensionLoader

from xml.dom.minidom import parseString, Document
from xml.parsers.expat import ExpatError

from tempfile import TemporaryFile
from imp import load_module as LoadModule, PY_SOURCE

from sys import stderr

from hashlib import md5

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

	for i in ['plan']:
		assert i in plan, 'Attribute %s not in plan' % (i)

	assert md5(plan['plan']).hexdigest() == plan['checksum'], (
		'Computed MD5 does not match the one in the request')

	return (targets, plan)

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# # # # # # # # # # # # # End of Auxiliary Functions  # # # # # # # # # # # # #
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

def persistPlan(txn, targets, plan):
	txn.execute(
		'INSERT INTO healing_plans VALUES (?, ?)',
		(plan['checksum'], plan['plan']))

	for t in targets:
		txn.execute(
			'INSERT INTO healing_targets VALUES (?, ?, ?, ?)'
			(plan['md5'], t['address'], t['port'], t['transport'])
		)

		nottorecord = {
			'md5': None, 'address': None,
			'port': None, 'transport': None
		}

		for k,v in t.itervalues():
			txn.execute(
				'INSERT INTO healing_data VALUES (?, ?, ?, ?, ?, ?)',
				(	plan['md5'], t['address'],
					t['port'], t['transport'],
					str(k), str(v)
				)
			) if k not in nottorecord else None

	return targets, plan

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
		'SELECT peer FROM healing_group ORDER BY peer'
	).fetchall()

	writer = BundleWriter('dtnhealing:')

	for p in peers:
		writer.write(p['peer'], m.toxml())

	return targets, plan

def startHealingPlan(txn, message, header):
	faulty = None
	errors = None

	try:
		faulty = dict (
			(str(k), str(v)) for k,v in
				data.getElementsByTagName('faulty')[-1].attributes.items() )

		errors = dict (
			(str(k), str(v)) for k,v in
				data.getElementsByTagName('erros')[-1].attributes.items() )

	except IndexError as e:
		print >> stderr, "Missing element in fault notification"

	healingPlan = txn.runQuery('''
		SELECT plan FROM healing_targets NATURAL JOIN healing_plans WHERE
			address = ? AND port = ? AND transport = ?''',
		(faulty['address'], faulty['port'], faulty['transport']))

	healingData = dict(
		map(lambda r: (str(r['attribute']), str(r['data'])),
			txn.runQuery('''
				SELECT attribute,data FROM healing_data WHERE
					md5 = ? AND address = ? AND port = ? AND transport = ?''',
				(healingPlan['md5'], faulty['address'],
					faulty['port'], faulty['transport']))))

	healingPlan['module'] = TemporaryFile()
	healingPlan['module'].write(healingPlan['plan'])

	LoadModule(
		'healing-dtn-plan' + healingPlan['md5'],
		healingPlan['module'],
		'healing-dtn-plan' + healingPlan['md5'],
		('py0', '', PY_SOURCE)
	).execute(
		faulty, healingData, errors
	).addCallbacks(
		# Error was healed
		lambda r: dtnExtension.bundles.runInteraction(
			monitoringKeepGoing, faulty),

		# Error goes on
		lambda r: dtnExtension.bundles.runInteraction(
			monitoringDrop, faulty),
	)

def DTNHealingParser(header, content):
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
				lambda r: dtnExtension.bundles.runInteraction(
					persistPlan, r
				).addBoth(
					lambda r: True)
			).addBoth(
				lambda r: True)

			n.setAttribute('status', 'accepted')

		except AssertionError as e:
			print >> stderr, (
				'Rejecting a DTN healing request. Cause: %s' % (str(e)))

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
			startHealingPlan,
			message, header
		).addBoth(
			lambda r: True)

	elif kind == 'join group':
		source == header['Content-Source']

		dtnExtension.bundle.runQuery(
			'INSERT INTO healing_group VALUES (?, ?);',
			(source, time())
		).addCallback(
			lambda r: True
		).addErrback(
			lambda r: dtnExtension.bundle.runQuery(
				'UPDATE healing_group SET join_date = ? WHERE peer = ?;',
				(time(), source)
			).addBoth(
				lambda r: True)
		).addBoth(
			lambda r: True)

	elif kind == 'part group':
		source == header['Content-Source']

		dtnExtension.bundle.runQuery(
			'DELETE FROM healing_group WHERE peer = ?',
			(source,)
		).addBoth(
			lambda r: True)

# Bureaucratic stuff
extName = 'DTNHealing'

def extensionName():
	return extName

def extendProtocol(lFactory, sFactory):
	global dtnMonitors, BundleWriter

	try:
		if ExtensionLoader().isActive("Groups") is False:
			raise KeyError, "Group extension not active"

		if ExtensionLoader().isActive("HTTP-DTN") is False:
			raise KeyError, "HTTP-DTN extension not active"

		if ExtensionLoader().isActive("DTNHealingDataModel") is False:
			raise KeyError, "DTNHealingDataModel was not loaded"

		dtnMonitors = (ExtensionLoader().
						getExtension("Groups").
							getGroup("DTN-Healers"))

		dtnExtension = ExtensionLoader().getExtension("HTTP-DTN")

		BundleWriter = dtnExtension.BundleWriter

	except KeyError as e:
		return False

	dtnExtension.addToken('dtnhealing', DTNHealingParser)

	return True
