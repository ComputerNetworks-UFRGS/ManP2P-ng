# -*- coding: utf-8 -*-
from twisted.internet import defer

from commandLineParser import CommandLineParser
from extensionLoader import ExtensionLoader

# The sensors that accompanies this manager
sensorsList = [
	#('143.54.12.86',  22, 'q1w2e3r4t5'),	# Kashyyyk
	#('143.54.12.116', 22, 'q1w2e3r4t5'),	# Dagobah
	#('143.54.12.184', 22, '192168060'),		# Alderan
	#('143.54.12.112', 22, 'q1w2e3r4t5'),	# JEdiVM2
	# ('143.54.12.123', 22, 'q1w2e3r4t5'),	# JEdiVM1
	# ('143.54.12.160', 22, 'q1w2e3r4t5'),	# JEdiVM3
	# ('143.54.12.193', 22, 'q1w2e3r4t5'),	# JEdiVM0
	# ('143.54.12.86',  22, 'q1w2e3r4t5'),	# Kashyyyk
]
# How many of them have told us that they are back
nonConfirmed = len(sensorsList) + 1

# To other sensors to signal
hostDefer = None
# To signal back to the healing service
mainDefer = None

# Command to run on the remote host
command = '/etc/init.d/ossec restart'

def confirmationReply(result):
	global nonConfirmed, mainDefer

	if result.split()[-1] == 'Completed.':
		nonConfirmed -= 1

		if nonConfirmed == 0:
			mainDefer.callback(True)

def execute(host, port, df):
	global mainDefer

	mainDefer = df
	sshRPC = ExtensionLoader().getExtension("sshRPC").sshRPC
	sshRPCGroup = ExtensionLoader().getExtension("Groups").getGroup("sshRPC")

	runners = sshRPCGroup.getSubgroup(len(sshRPCGroup)-1)
	nRunners = len(sshRPCGroup)-1
	i = 0

	for sensor,port,passwd in sensorsList + [ (host, port, '192168061') ]:
		hostDefer = defer.Deferred()

		hostDefer.addErrback(df.errback)
		hostDefer.addCallback(confirmationReply)

		print "Asking to", runners[i % nRunners].nick, "to run the command"
		sshRPC(runners[i % nRunners], hostDefer,
				sensor, port, 'root', passwd, command)
		i += 1

