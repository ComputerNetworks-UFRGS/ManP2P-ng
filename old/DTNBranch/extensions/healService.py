# -*- coding: utf-8 -*-

from twisted.internet import reactor
from twisted.internet import defer

# PyOverlay Includes
from commonRegex import IPHostRegex,HostNameRegex
from extensionLoader import ExtensionLoader

# Python Library Includes
from tempfile import TemporaryFile
from imp import load_module as LoadModule
from random import randint
from imp import PY_SOURCE
from math import log

from hashlib import md5

import traceback

extname = 'HealService'
per = 'heal:((r:' + IPHostRegex + ':[0-9]+)|(p:[0-9]+@' + HostNameRegex + ':.+))'

startRegex = 'heal:(({0})|({1}))'.format(
	'r:{0}:{1}'.format(IPHostRegex, '[1-9][0-9]*'),
	'p:{0}@{1}:.+'.format('[1-9][0-9]*', HostNameRegex)
)

establishedRegex = 'heal:({0})'.format(
	'activate:{0}@{1}'.format('[1-9][0-9]*', HostNameRegex)
)

healingGroup = None

healings = { }

class HealService:
	def __init__(self, hid, clientHost, servicePort, healPlans=None):
		self.hid = hid

		self.clientHost = clientHost
		self.servicePort = servicePort
		self.otherNames = [ ]

		self.healPlans = None
		self.planHash = None
		self.fileName = None
		self.name = None

	# # # # # # # # # # # # # # # # # # # # #
	# Add a plan to a heal service request	#
	#										#
	#	It tries to parse the file and if	#
	#	succeed, send a notification for	#
	#	the client in the form:				#
	#										#
	#		heal:p:HID:a					#
	#										#
	#	else, he notifies the client with	#
	#	a message in the format:			#
	#										#
	#		heal:p:HID:e:MESSAGE			#
	#										#
	#	and destroys the healing plan		#
	#										#
	# # # # # # # # # # # # # # # # # # # # #
	def addPlan(self, rawPlan, sParser=None):
		if self.healPlans is not None:
			sParser.write('heal:p:e:alreadyInPlans')
			return

		# Let's put the data in a file. As we are
		# doing this in a separated thread, we can
		# lost some time and the other side wait
		self.plan = TemporaryFile()
		self.plan.write(rawPlan)

		# And now, does it parse? Does it really
		# a Python source code? Please, don't
		# waste more time :-/
		try:
			tm = self.getExecutablePlan()
			# Ok, it's Python source code. But
			# let's let it in the disk for saving
			# main memory. When needed, we may
			# reload it.
			del tm

		except Exception as e:
			# OMG! Not Python code...
			# Let's clean the house
			# and let it be
			print e

			for i in rawPlan.split('\n'):
				print i

			self.plan.close()

			# but first, let's blame the guy
			if sParser is not None:
				sParser.write('heal:p:e:{0}:syntaxError'.format(self.hid))
				del self
				return

			raise Exception, "Can't parse the plan"

		# Great, the plan is executable code.
		# Now it needs to be persisted
		self.planHash = md5(rawPlan)

		# sParser will be None if the plan was
		# sent by other group member. If so,
		# we shall not give a reply and forward
		# for other peers
		if sParser is not None:
			# Oh yes! Hi's a lucky guy...
			# Let's let him knows :-)
			sParser.write("heal:p:accepted:{0}".format(self.hid))

			# Now, it's time to distribute the workload
			# between the other healer. We'll sort lon(n)
			# group members to help us... But not in this
			# version :-)

			# First, let's give a look in what we are
			# forwarding
			(healingGroup.
					sendMessage('s:' + str(self), messageCallback))
					# Uncommenting the following lines and erasing the last
					# parenthesis of the previous statement will make the
					# system choose log(n) other peers to receive the plan.
					# But, as we don't have a good sync strategy for frequent
					# churns, we'll send the message to everyone.
					#
					#, lambda x:x,
								#healingGroup.
									#getSubgroup(len(healingGroup),
												#lambda x: log(x,2)-1)))

	def getExecutablePlan(self):
		self.plan.seek(0)

		if not self.name:
			self.name = "healM-tmp-" + str(randint(0,1000000))

		return LoadModule(self.name, self.plan,
							self.name, ('pyO', '', PY_SOURCE))

	def execute(self, activationMessage):
		self.activationMessage = activationMessage
		
		d = defer.Deferred()
		d.pause()
		d.addBoth(self.activationMessageReply)

		self.cplan = self.getExecutablePlan()

		reactor.callFromThread(self.cplan.execute,
								self.clientHost, self.servicePort, d)

		d.unpause()

	def activationMessageReply(self, how):
		del self.cplan

		print "Replying to the guy"

		which = self.activationMessage[2].split(':')[2]
		peer = self.activationMessage[3]

		if how == True:
			print "And we are back!"
			peer.write("monitoring:resume:" + which)
		else:
			print "The plan failed... :-("

	def mkFName(self):
		if not self.fileName:
			self.fileName = ('healPlan-for-' + self.clientHost +
								':' + str(self.servicePort) + '.hp')

		return self.fileName

	def __str__(self):
		self.plan.seek(0)
		rstring = ":".join([str(self.hid), self.clientHost,
							str(self.servicePort), self.planHash.hexdigest(),
							self.plan.read()])
		self.plan.seek(0)
		return rstring

	# # # # # # #
	# Auxiliary #
	# # # # # # #
	def __del__(self):
		if self.healPlans is not None:
			self.healPlans.close()
		if self.planHash is not None:
			del self.planHash
		if self.fileName is not None:
			# Shall delete the file
			# on exit. Serious
			pass

		del self.healPlans
		del self.otherNames
		del self.clientHost
		del self.servicePort

def messageCallback(group, sParser, msg):
	print msg

def healGroupCallback(group, sParser, msg):
	print msg

	data = msg[2].split(':')

	if data[0] == 's':
		rid, tHost, tPort, pHash = data[1:5]

		plan = ":".join(data[5:])

		healings[rid] = HealService(rid, tHost, int(tPort))

		# We'll recheck the plan for errors.
		try:
			healings[rid].addPlan(plan)
		except:
			# Have errors. Why? we don't know
			# Not useful for the "make it work"
			# project's phase
			pass

		if healings[rid].planHash.hexdigest() != pHash:
			print "Plans hash don't match"

		healingGroup.replyMessage(sParser, msg, "a:ok")

	elif data[0] == 'activate':
		try:
			plan = healings[data[1]]
			plan.execute(msg)

		except KeyError:
			print "I know nothing about it :-("

		except:
			traceback.print_exc()


	del data

def healTokenParse(sParser, t):
	# # # # # # # # # # # # # # # # # # # # #
	# The client is asking for the service  #
	#										#
	# Example:								#
	#	Client says:						#
	#		heal:r:143.54.12.86:80			#
	#										#
	#	Peer responds:						#
	#		heal:p:a:HID					#
	#			where HID is a the id of	#
	#			the service provided.		#
	#										#
	# # # # # # # # # # # # # # # # # # # # #
	if t[1] == 'r':
		# First things first, let's check if the
		# the service port is valid.
		try:
			# If t[3] isn't a integer, a ValueError
			# exception will be risen.
			servicePort = int(t[3])

			if servicePort < 0 or servicePort > 65535:
				raise ValueError, "Port parameter not in regular bounds"

		# If it's not, send a message informing
		# the other side.
		except ValueError:
			sParser.write("heal:p:e:portParameter")
			return

		# Finishing with verifications, let's
		# check if we already have something
		# to the specified IP/Port tuple
		for i in healings.itervalues():
			if i.clientHost == t[2] and i.servicePort == servicePort:
				sParser.write("heal:p:e:alreadyInPlans")
				return

		# Now, let's get a unique ID for this healing
		# service. Theoretically, this procedure is
		# very, very inefficient. But we shall have
		# faith in The Great God of Randomness and
		# expects that it returns in O(1) :-)
		rint = None
		while rint == None:
			rint = str(randint(0,1000000)) + '@' + sParser.myNick

			if rint in healings:
				rint = 0
			else:
				# Do the obvious
				hp = healings[rint] = HealService(rint, t[2], servicePort)

		# Let's say to the client that we accept
		# his service request \o/ Lucky guy!
		sParser.write("heal:p:a:{0}".format(rint))

	# # # # # # # # # # # # # # # # # # # # # # #
	# The client is sending his healing plan 	#
	#											#
	# Example:									#
	#	Cliente says:							#
	#		heal:p:HID:PLAN						#
	#			Wheres HID is heal service id	#
	#			sent from the peer to the		#
	#			client and PLAN is the source	#
	#			of the plan to be applied.		#
	#											#
	#	Peer says:								#
	#		help:p:accepted						#
	#			if the plan is correctly parsed #
	#											#
	# # # # # # # # # # # # # # # # # # # # # # #
	if t[1] == 'p':
		# Is the parameter really a integer?
		try:
			hid = t[2]
		except ValueError:
			# No, it is not! Let's blame this guy
			sParser.write("heal:e:invalidParameter:" + t[2])
			return

		# Ok, it's a integer... But is it a
		# valid healing plan?
		if hid not in healings:
			# What? Is he trying to cheat on us?
			# Blame him, blame him! As a bonus for
			# the inconvenient, we'll close the
			# connection! Bye, looser!
			sParser.write("heal:e:noSuchHealingPlan")
			sParser.disconnect()

			return

		# We shall not block |ô/ ;-)
		#	So, let's parse the file in a different thread.
		#	This thread get the responsibility to accept or
		#	drop the plan. Any issue related to not conformity
		#	with the standards, is treated inside this
		#	function! Cross your fingers...
		reactor.callFromThread(healings[hid].addPlan,
								':'.join(t[3:]), sParser)

# # # # # # # # # # # # #
# Burocratic steps :-) #
# # # # # # # # # # # #
def extensionName():
	return extname

def extendProtocol(lexicalFactory, syntaxFactory):
	global healingGroup

	# We do depend on group communication.
	# Let's see if it's active
	if not ExtensionLoader().isActive("Groups"):
		return False

	# Wow, we have group communication. But,
	# are we in healGroup?
	try:
		healingGroup = (ExtensionLoader().
							getExtension("Groups").
								getGroup("healGroup"))
	except KeyError:
		# :-( we are not working with this extension
		# Let's say to the core to not waste it's
		# precious processor time and memory with us
		return False

	healingGroup.addCallback(healGroupCallback)
	syntaxFactory.addToken('heal', healTokenParse)
	(lexicalFactory.
			getState('start').
					addTransition('start', per,
									lambda t: t.split(':')))

	# Oh yes, baby! Everything is
	# ready. So, we have to
	return True
