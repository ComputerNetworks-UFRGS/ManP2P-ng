# -*- coding: utf-8 -*-

from twisted.protocols.basic import NetstringReceiver
from twisted.internet.protocol import ClientCreator

from twisted.internet import reactor

from sys import argv,exit

hsid = None

class PlanSender(NetstringReceiver):
	def __init__(self, requestingHeader,
							requestingArguments,
						requestingReplyParser,
						plan):

		self.requestingHeader = requestingHeader
		self.requestingArguments = requestingArguments

		self.requestingReplyParser = requestingReplyParser

		self.plan = plan

		self.thisIsTheEnd = False

	def connectionMade(self):
		print "-+- Connection Made -+-"
		print "\tAsking a monitoring slot"

		# Requisita o plano
		self.sendString(
			self.requestingHeader.format(**self.requestingArguments)
		)

	def stringReceived(self, data):
		print data

		r = self.requestingReplyParser(data,self)

		if r is not None:
			self.sendString(r)

	def connectionLost(self, reason):
		print "-+- Connection Lost -+-"

		if self.thisIsTheEnd:
			reactor.stop()

if len(argv) < 3:
	print "Misuse"
	exit(1)

try:
	planSource = open(argv[3])
except Exception as e:
	print e


def monitoringPlanReplyParser(data, c):
	t = data.split(':')

	if t[0] != 'monitoring':
		return None

	if t[1] == 'r':
		if t[2] == 'a':
			return 'monitoring:p:{0}:{1}'.format(t[3], c.plan)

	elif t[1] == 'p':
		if t[2] == 'a':
			c.thisIsTheEnd = True
			return 'monitoring:c:{0}:{1}'.format(t[3], hsid)

	return None

monitoringArguments = {
	'requestingHeader': 'monitoring:r:{host}:{port}:{period}',

	'requestingArguments': {
			'host': '143.54.12.74',
			'port': 22,
			'period': 5
	},

	'requestingReplyParser': monitoringPlanReplyParser,
	'plan': open(argv[3],'r').read(),
}

def healPlanReplyParser(data, c):
	t = data.split(':')

	if t[0] != 'heal' or t[1] != 'p':
		return None

	if t[2] == 'a':
		return 'heal:p:{0}:{1}'.format(t[3], c.plan)

	if t[2] == 'accepted':
		global hsid
		hsid = t[3]

		ClientCreator(reactor, PlanSender,
						**monitoringArguments).connectTCP(
												argv[1], int(argv[2]))

healArguments = {
	'requestingHeader': 'heal:r:{host}:{port}',

		'requestingArguments': {
			'host': '143.54.12.74',
			'port': 22,
		},

	'requestingReplyParser': healPlanReplyParser,
	'plan': open(argv[6],'r').read()
}

ClientCreator(reactor, PlanSender, **healArguments).connectTCP(
												argv[4], int(argv[5]))

reactor.run()