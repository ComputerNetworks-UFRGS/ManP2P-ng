# -*- coding: utf-8 -*-

from extensionLoader import ExtensionLoader
from commandLineParser import CommandLineParser

from overlay import addHaltFunction as overlayAHF

from commonRegex import IPHostRegex, GroupNameRegex

from time import time
from random import randint,sample

groups = { }

# # # # # # # # # # # # # # # # # #
# Get a group based on it's name #
# # # # # # # # # # # # # # # # #
def getGroup(groupName):
	return groups[groupName]

#class Message:
	#def __init__(self, mid, cback=None):
		#self.id = mid
		#self.cback = cback

	#def doCallback(self, reply):
		#self.cback(self, reply)

class Group:
	def __init__(self, name, amIn=False, itens=None, mySelf = ''):
		self.amIn = amIn
		self.name = name
		self.mySelf = mySelf

		if amIn:
			self.callbacks = [ ]

			self.sentMessagesID = { }
			self.recvMessagesID = [ ]
			self.messageCleaner = None

			self.myNick = CommandLineParser().getArguments().PeerName
		else:
			self.callbacks = None

		if itens is not None:
			self.hosts = itens
		else:
			self.hosts = [ ]

	def addPeer(self, remotePeer):
		if remotePeer not in self.hosts:
			self.hosts.append(remotePeer)
			remotePeer.getAnotation("groups").append(self.name)

	# # # # # # # # # # # # # # # # # # # # # # #
	# Choose a random subgroup of size "size".	#
	# "f" can be used to shape the size.		#
	# # # # # # # # # # # # # # # # # # # # # # #
	def getSubgroup(self, size=0, f=lambda x: x):
		# The seize is normalized till the bones!
		# No exceptions allowed!
		return sample(self.hosts, max(0, int(round(f(size)))))

	def getMember(self, member):
		return [h for h in self.hosts if h == member ][0]

	# # # # # # # # # # # # # # # # #
	# Reserves a message to be send #
	# # # # # # # # # # # # # # # # #
	def getMessageSlot(self, cback=None):
		# Inefficient method to get a ID for
		# a message so when can play with it
		# like calling callbacks when someone
		# replies
		while True:
			rint = randint(0,1000000)

			# Let's add the ID, the callback registered
			# and at what time it was sent so we can
			# periodically clean up the list...
			if rint not in self.sentMessagesID:
				self.sentMessagesID[rint] = (cback, time())
				return rint

		raise Exception, "No free message slot available"

	# # # # # # # # # # # # # # # # # # # # # # #
	# Send a message to all hosts in			#
	# the group or to a subgroup, if			#
	# sgroup is set								#
	#											#
	#	Raises: Exception if message can't		#
	#	be sent									#
	# # # # # # # # # # # # # # # # # # # # # # #
	def sendMessage(self, message, cback=None, sgroup=None):
		mid = self.getMessageSlot(cback)

		if sgroup is None:
			sgroup = self.hosts

		# Iterating through the hosts in this group
		# to deliver them the message. Unfortunately,
		# we have to use application level multicast.
		for p in sgroup:
			p.transport.write("group:msg:" + self.name + ":" +
								str(mid) + ":s:" + message)

	# # # # # # # # # # # # # # # #
	# Reply to a message received #
	# # # # # # # # # # # # # # # #
	def replyMessage(self, sParser, qmsg, message):
		sParser.write("{0}:{1}:{2}:{3}:{4}:{5}".format(
					"group", "msg", self.name, qmsg[0], "a", message))

	# # # # # # # # # # # # # # # # # # # # # # #
	# Reply received							#
	#											#
	#	This function is passively called when	#
	#	a message is received and shall not be	#
	#	called by the user.						#
	#											#
	# # # # # # # # # # # # # # # # # # # # # # #
	def messageReply(self, sParser, msID, message, cback=None):
		# If we actually received this message,
		# we would remember it. Let's check
		if msID in self.sentMessagesID:
			# Wow, found it... But does it have
			# a associate callback? Let's assume
			# yes and treat errors as exceptions
			try:
				self.sentMessagesID[msID][0](self, sParser, message)
			except TypeError as e:
				print e
				# So, no callback. A answer was not expected,
				# but, what should we do in this case? For the
				# moment, let's just
				pass

			del self.sentMessagesID[msID]
		else:
			# If we reach this point, we are dealing
			# with looser... Let's ignore him?
			pass

	# # # # # # # #
	# Callbacking #
	# # # # # # # #
	def addCallback(self, f):
		self.callbacks.append(f)

	def doCallback(self, sParser, msg):
		for f in self.callbacks:
			if f(self, sParser, msg) is True:
				return True

		return False

	# # # # # # # # # # # # #
	# Messages list cleanup	#
	# # # # # # # # # # # # #
	def clenUp(self):
		pass

	def removePeer(self, remotePeer):
		if remotePeer in self.hosts:
			self.hosts.remove(remotePeer)

	# # # # # # #
	# Iterator #
	# # # # # #
	def __iter__(self):
		return Group(self.name, self.amIn, self.hosts[:])

	def next(self):
		try:
			return self.hosts.pop()
		except IndexError:
			raise StopIteration

	# # # # # #
	# Helpers #
	# # # # # #
	def __str__(self):
		return ','.join(([self.myNick] if self.amIn else []) +
								map(lambda s: s.nick, self.hosts))

	def __len__(self):
		return len(self.hosts) + int(self.amIn)

	def __contains__(self, remotePeer):
		if remotePeer in self.hosts:
			return True

		return False

	def __del__(self):
		del self.hosts
		del self

# # # # # # # # # # # # # # # # # # # # # #
# Called when the connection starts so   #
# the other sides knows which groups we #
# are in.                              #
# # # # # # # # # # # # # # # # # # # #
def joinGroups(sParser):
	# To annotate the remote peers groups
	sParser.remotePeer.setAnotation("groups", [ ])

	# Let's say to him which groups we are in
	for g in CommandLineParser().getArguments().groupsToJoin:
		sParser.write('group:j:' + g)

# # # # # # # # # # # # # # # # # # # # # #
# Called when the connection finishes so #
# we have a consistent view of peers in #
# the groups.                          #
# # # # # # # # # # # # # # # # # # # #
def removeFromGroups(overlay):
	if overlay.sParser.remotePeer is None:
		return

	for g in overlay.sParser.remotePeer.getAnotation("groups"):
		print ("Removing " + overlay.sParser.remotePeerNick +
				" from group " + g)

		groups[g].removePeer(overlay.sParser.remotePeer)

		if len(groups[g]) == 0:
			del groups[g]

def groupTokenParser(sParser, t):
	# # # # # # # # # # # # # # # # # # # # #
	# Incoming message to a group			#
	#										#
	#	format:								#
	#		..0..:.1.:..2..:.3:...4..:.5.	#
	#		group:msg:GROUP:ID:STATUS:MSG	#
	#										#
	# # # # # # # # # # # # # # # # # # # # #
	if t[1] == 'msg' and len(t) >= 4:
		# First of all, let's check if receiving
		# this message has anZçy logic. Or, if we
		# did join the group.
		if t[2] not in groups or groups[t[2]].amIn is False:
			# Misbehave detected ¬¬ # Shall we react?
			# May be next time... For the moment, just
			# halt...
			sParser.write("group:msg:" + t[2] + ":" + t[3] + ":e:\0")
			return

		# Ok, we are in the group...
		# Let's handle the message

		# Someone is asking something
		if t[4] == 's':
			groups[t[2]].doCallback(sParser,
									# tuple = (MsID, STATUS, MSG, sParser)
									(int(t[3]), t[4], ":".join(t[5:]), sParser)
								)

		# Wow, someone got a answer to us...
		# But, did we ask something? We'll see
		elif t[4] == 'a':
			groups[t[2]].messageReply(sParser, int(t[3]), ":".join(t[5:]))

		# Something gone wrong
		elif t[4] == 'e':
			pass

	# # # # # # # # # # # # # # #
	# Peer group join request	#
	#							#
	#	Format:					#
	#		..0..:1:..2..		#
	#		group:j:GROUP		#
	#							#
	# # # # # # # # # # # # # # #
	elif t[1] == 'j' and len(t) == 3:
		print ("Peer {0} requested to join group {1}".
				format(sParser.remotePeerNick, t[2]))

		if t[2] not in groups:
			print "Group {0} does not exist. Creating it.".format(t[2])
			groups[t[2]] = Group(t[2])

		groups[t[2]].addPeer(sParser.remotePeer)

	# # # # # # # # # # # # #
	# Peer group part		#
	#						#
	#	Format:				#
	#		..0..:1:..2..	#
	#		group:p:GROUP	#
	#						#
	# # # # # # # # # # # # #
	elif t[1] == 'p' and len(t) == 3:
		print("Peer {0} is parting from group {1}".
				format(sParser.remotePeerNick, t[2]))

		for g in sParser.remotePeer.getAnotation("groups"):
			groups[g].removePeer(sParser.remotePeer)

	# # # # # # # # # # # # # # #
	# Peer group members lookup #
	#							#
	#	Format:					#
	#		..0..:1:..2..		#
	#		group:l:GROUP		#
	#							#
	# # # # # # # # # # # # # # #
	elif t[1] == 'l' and len(t) == 3:
		if sParser.remotePeerNick is not None:
			pname = sParser.remotePeerNick + ' '
		else:
			pname = ""

		# Debug message
		print "Peer {0} is requesting a lookup on group {1}".format(pname, t[2])

		# We don't know about this group
		# Let's the requestor knows
		if t[2] not in groups:
			sParser.write("group:e:noSuchAGroup")
			return

		# List for sending the peers of the
		# requested group
		grps = [ ]

		# If I'm in the group, put my address
		if groups[t[2]].amIn:
			grps.append(','.join(map(str, [sParser.transport.getHost().host] +
									CommandLineParser().getArguments().lport)))

		# Put the address of other peers in
		# the list
		for h in groups[t[2]]:
			grps.append(
					','.join(map(str, [h.peer] + h.getAnotation('lports'))))

		# Send the list to the requestor
		sParser.write("group:a:" + t[2] + ":" + ':'.join(grps))

		# We can't hold the memory
		# forever... Free!
		del grps

	elif t[1] == 'a' and len(t) > 3:
		groups[t[2]] = t[4].split(',')

	elif t[1] == 'e':
		pass

# # # # # # # # # # # # #
# Burocratic steps :-) #
# # # # # # # # # # # #
def extensionName():
	return "Groups"

def extendCommandLine(lexicalFactory, syntaxFactory):

	CommandLineParser().add_argument('-g', '--joinGroups',
							dest='groupsToJoin', nargs='+',
							default=[ ], metavar='GROUP',
							help='Peers groups to join')

def extendProtocol(lexicalFactory, syntaxFactory):
	# j == join
	# p == part
	# l == lookup
	# a == lookup answer
	regex = "group:(({0})|({1})|({2})|({3}))".format(
			"(j|l|p):{0}".format(GroupNameRegex),
			"a:{0}(:{1}(,[1-9][0-9]?)+)+".format(GroupNameRegex, IPHostRegex),
			"msg:{0}:[0-9]+:(s|a):.+".format(GroupNameRegex),
			"e:.*"
	)

	if ExtensionLoader().isActive("BasicOverlay"):
		BProtocol = ExtensionLoader().getExtension("BasicOverlay")
		BProtocol.addBootstrapFunction(joinGroups)

		establishedState = lexicalFactory.getState('established')
		establishedState.addTransition('established', regex,
										lambda t: t.split(':'))

		startState = lexicalFactory.getState('start')
		startState.addTransition('start', regex, lambda t: t.split(':'))

		syntaxFactory.addToken('group', groupTokenParser)
		overlayAHF(removeFromGroups, True)

		for g in CommandLineParser().getArguments().groupsToJoin:
			groups[g] = Group(g, True, None,
								CommandLineParser().getArguments().PeerName)

		return True

	return False
