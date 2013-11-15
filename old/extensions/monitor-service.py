# -*- coding: utf-8 -*-

from twisted.internet import reactor, defer

# Overlay Includes
from commonRegex import IPHostRegex, IPHost, HostNameRegex
from extensionLoader import ExtensionLoader

# Python Standard Library Includes
from random import randint

from tempfile import TemporaryFile

from imp import load_module as LoadModule
from imp import PY_SOURCE

from hashlib import md5
from time import time

extname = 'MonitorService'

mer = 'monitoring:(({0})|({1})|({2}))'.format(
	# r:THOST:TPORT:PRIOD
	'r:{0}:{1}:{2}'.format(IPHostRegex, '[0-9]+', '[0-9]+'),

	# p:MsID:MPLAN
	'p:{0}@{1}:{2}'.format('[0-9]+', HostNameRegex, '.+'),

	# c:MsID:HsID
	'c:{0}@{1}:{0}@{1}'.format('[0-9]+', HostNameRegex),
)

mest = 'monitoring:resume:{0}@{1}'.format('[0-9]+', HostNameRegex)

monitorPlans = { }

monitorGroup = None
healGroup = None
# The Monitoring Plan
class MonitorPlan:
	def __init__(self, plan):
		# Source code of the plan stored at a temporary
		# file. Auto removed.
		self.plan = None

		# The name of the file where the compiled bytecode
		# will reside. We shall remove this file by hand.
		self.name = None

		# The module resulted of the plan
		# compilation process
		self.cplan = None

		# Self-explained
		self.hash = None

		if plan is not None:
			self.registerPlan(plan)

	def registerPlan(self, plan):
		self.plan = TemporaryFile()
		self.plan.write(plan)
		self.plan.seek(0)

		try:
			tm = self.getExecutablePlan()
		except Exception as e:
			print e

			self.plan.close()

			del self.plan
			del self

			raise Exception, "Can't pase the plan " + str(e)

		del tm

		self.hash = md5(plan)

	def getExecutablePlan(self):
		self.plan.seek(0)

		if not self.name:
			self.name = "monitorM-tmp-" + str(randint(0,1000000))

		return LoadModule(self.name, self.plan,
							self.name, ('pyO', '', PY_SOURCE))

	# Execute the plan. Submitted plans must have a function
	# called 'execute' that receives three arguments: the target
	# host and port, and a defer. This was done this way thinking
	# in the reuse of monitoring plans. The plan 'execute' shall
	# callback in the defer when its execution finish.
	def execute(self, clienteHost, servicePort):
		d = defer.Deferred()
		d.pause()

		if self.cplan is None:
			self.cplan = self.getExecutablePlan()

		reactor.callFromThread(self.cplan.execute, clienteHost,
								servicePort, d)

		print "Returning from plan execute"
		return d

	def __str__(self):
		self.plan.seek(0)
		return "{0}:{1}".format(self.hash.hexdigest(), self.plan.read())

	def __nonzero__(self):
		return True if self.plan is not None else False

# Service Instance
class MonitorService:
	def __init__(self, msid, clienteHost, servicePort, period, plan=None):
		# This plan ID
		self.msid = msid
		# Associated heal plan
		self.hsid = None

		# The host targeted by this monitoring group,
		# the port in this host and the period of the
		# plan. This info will be spread to other peers
		self.clienteHost = clienteHost
		self.servicePort = servicePort
		self.period = period

		# Instantiating the plan
		self.plan = MonitorPlan(plan)

		# The monitoring ring.
		self.rPeers = [ ]
		self.nextPeer = None

	def addPlan(self, rawPlan, sParser=None):
		if self.plan:
			sParser.write('monitoring:p:e:alreadyInPlans')
			return

		try:
			self.plan.registerPlan(rawPlan)

		except Exception as e:
			if sParser is not None:
				sParser.write('monitoring:p:e:{0}:syntaxError'.
											format(self.msid))

			del self
			raise e

		if sParser is not None:
			# This message will tell other peers where is their
			# positions on the monitoring ring of this plan.
			sMessage = 'synchronize:{0}:{1}'.format(
								self.msid, str(monitorGroup))

			# This message tells to the requester that the plan
			# was accepted and will be applied
			sParser.write('monitoring:p:a:' + self.msid)

			# Now, this message will distributed the plan to
			# the other peers
			monitorGroup.sendMessage('newplan:' + str(self))
			# Sends the message about the monitoring ring
			monitorGroup.sendMessage(sMessage)
			# Auto message for setting up the monitoring ring
			# next peer. Non-Standard Technical Solution
			monitorGroupCallback(monitorGroup, None, (0, None, sMessage))

	def checkAndPass(self):
		try:
			b = self.plan.execute(self.clienteHost, self.servicePort)
		except Exception as e:
			print e
			raise e

		b.addCallbacks(
			callback=self.successToken,
			errback=self.unsuccessToken
		)

		b.unpause()

		print "Callbacks added and returning"

	def reCheck(self):
		try:
			b = self.plan.execute(self.clienteHost, self.servicePort)
		except Exception as e:
			print e
			raise e

		b.addCallbacks(
			callback=self.schedulePlan,
			errback=self.activateHealingService
		)

		b.unpause()

		print "Callbacks added and returning"

	def successToken(self, t):
		monitorGroup.sendMessage("token:{0}:ok".format(self.msid),
									None, [self.nextPeer])
		print "Sending successToken"

	def unsuccessToken(self, t):
		monitorGroup.sendMessage("token:{0}:nok".format(self.msid),
									None, [self.nextPeer])
		print "Sending unsuccessToken"

	def schedulePlan(self, t):
		reactor.callLater(self.period, self.checkAndPass)

	def activateHealingService(self, t):
		global healGroup

		self.time = time()

		if healGroup is None:
			healGroup = (ExtensionLoader().
							getExtension("Groups").
								getGroup("healGroup"))

		healGroup.sendMessage("activate:{0}:{1}".format(self.hsid,self.msid),
								None, healGroup.getSubgroup(1))

		print "We were unsuccessful /o\\ Let's call the healers!"

	def __str__(self):
		return "{0}:{1}:{2}:{3}:{4}".format(
						self.msid, self.clienteHost, self.servicePort,
						self.period, self.plan)

def monitorGroupCallback(group, sParser, msg):
	print "\n-+- MonitorGroup message received -+-\n", msg, '\n'

	data = msg[2].split(':')

	# Parse plans sent by other peers. The message
	# will be described in the following format:
	#
	#	..0..:..1.:..2..:..3..:..4..:.5.:..6..
	#	TOKEN:MSID:THOST:TPORT:PRIOD:md5:MPLAN
	if data[0] == 'newplan':
		msid, tHost, tPort, pPeriod, pHash = data[1:6]

		plan = ":".join(data[6:])

		try:
			monitorPlans[msid] = MonitorService(msid, tHost, int(tPort),
												int(pPeriod), plan)
		except Exception as e:
			pass

		# Let's tell the sender that we successfully parsed
		# the message and that we accept to help him with this
		# monitoring plan
		if sParser:
			group.replyMessage(sParser, msg,
									"newplan:{0}:accepted".format(data[1]))

	# Parsing of synchronization message. These message will
	# be in the following format:
	#
	#	.......0.......:.1.:..2..
	#	synchronization:mid:peers
	elif data[0] == 'synchronize':
		plan = monitorPlans[data[1]]

		plan.rPeers = data[2].split(',')
		plan.nextPeer = monitorGroup.getMember(
						plan.rPeers[(plan.rPeers.index(monitorGroup.mySelf)+1)
										% len(group)])

		if not sParser:
			reactor.callLater(plan.period, plan.checkAndPass)

	elif data[0] == 'biding':
		monitorPlans[data[1]].hsid = data[2]

	# Receive a monitoring token from the other peer, verify
	# if it's ok and schedule the next monitoring. The message
	# will be in the following format:
	#
	#	..0..:.1.:.2.
	#	token:mid:sta
	elif data[0] == 'token':
		plan = monitorPlans[data[1]]

		if data[2] == 'nok':
			plan.reCheck()
		else:
			reactor.callLater(plan.period, plan.checkAndPass)

		group.replyMessage(sParser, msg, "token:received")

	del data

def monitoringTokenParser(sParser, t):
	# 1:--2--:--3--:--4--
	# r:THOST:TPORT:PRIOD
	if t[1] == 'r':
		# First check: are we talking the same
		# protocol?
		if len(t) != 5:
			sParser.write("monitoring:r:e:parametersMismatch")
			return

		try:
			# Verifying the port parameter
			tPort = int(t[3])

			if tPort < 0 or tPort > 65535:
				raise ValueError, "Port parameter not in regular bounds"

			# Verifying the period parameter
			pProd = int(t[4])

			if pProd < 0:
				raise ValueError, "Period parameter not in regular bounds"

			# Verifying the host parameter
			if not IPHost.match(t[2]):
				raise ValueError, "Host parameter not valid"

		except ValueError as e:
			print e
			sParser.write("monitoring:r:e:parametersNotInBounds")
			return

		for m in monitorPlans.itervalues():
			if m.clienteHost == t[2] and m.servicePort == tPort:
				sParser.write("monitoring:r:e:alreadyInPlans")
				return

		rint = None
		while rint is None:
			rint = str(randint(0,1000000)) + '@' + sParser.myNick

			if rint in monitorPlans:
				rint = None
			else:
				monitorPlans[rint] = MonitorService(rint, t[2], tPort, pProd)

		sParser.write("monitoring:r:a:" + rint)

	# 1:-2--:--3--
	# p:MsID:MPLAN
	if t[1] == 'p':
		if len(t) < 4:
			sParser.write("monitoring:p:e:parametersMismatch")
			return

		msid, peer = t[2].split("@")

		if peer != sParser.myNick:
			sParser.write("monitoring:p:e:invalidMsID")
			return

		try:
			reactor.callFromThread(monitorPlans[t[2]].addPlan,
									':'.join(t[3:]), sParser)
		except ValueError or KeyError:
			sParser.write("monitoring:p:e:noSuchMonitoringPlan")
			sParser.disconnect()

			return

	# 1:-2--:--3-
	# c:MsID:HsID
	if t[1] == 'c':
		try:
			monitorPlans[t[2]].hsid = t[3]
			monitorGroup.sendMessage('biding:{0}:{1}'.format(t[2],t[3]))
		except KeyError:
			sParser.write("monitoring:c:e:invalidMsID")
			sParser.disconnect()

	if t[1] == 'resume':
		print "Healing Time: ", str(time() - monitorPlans[t[2]].time)
		monitorPlans[t[2]].schedulePlan(True)

	if t[1] == 'e':
		pass

# # # # # # # # # # # # #
# Burocratic steps :-) #
# # # # # # # # # # # #
def extensionName():
	return extname

def extendProtocol(lexicalFactory, syntaxFactory):
	global monitorGroup, healGroup

	if not ExtensionLoader().isActive("Groups"):
		return False

	try:
		monitorGroup = (ExtensionLoader().
							getExtension("Groups").
								getGroup("monitoringGroup"))
	except KeyError:
		return False

	monitorGroup.addCallback(monitorGroupCallback)

	(lexicalFactory.
		getState('start').
			addTransition('start', mer, lambda t: t.split(':')))

	(lexicalFactory.
		getState('established').
			addTransition('established', mest,
				lambda t: t.split(':')))

	syntaxFactory.addToken('monitoring', monitoringTokenParser)

	return True