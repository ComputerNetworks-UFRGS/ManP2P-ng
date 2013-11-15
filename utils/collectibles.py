# -*- coding: utf-8 -*-

from twisted.internet.task import LoopingCall
from twisted.internet import reactor

from time import time

class ITimeCollectible(dict):
	time = { }

	scheduling = None

	gracetime = 0
	period = 0

	def __init__(self, timeout=0, period=0, *args, **kwargs):
		dict.__init__(self, *args, **kwargs)

		self.gracetime = timeout
		self.period = period

		if period > 0:
			self.scheduleTimeouts(period)

	def timeout(self, k):
		raise NotImplementedError

	def collect(self):
		timenow = time()
		timeouted = [ ]

		for k,v in self.iteritems():
			if timenow - self.time[k] > self.gracetime:
				timeouted.append(k)

		for k in timeouted:
			self.time[k] = timenow
			self.timeout(k)

	def forget(self, k):
		del self[k]
		del self.time[k]

	def scheduleTimeouts(self, period=0, now=False):
		assert period >= 0, 'Invalid period input: ' + str(period)

		if self.scheduling is not None:
			self.scheduling.stop()
			del self.scheduling

		if period == 0:
			if self.period != 0:
				period = self.period
			else:
				return False

		self.scheduling = LoopingCall(self.collect).start(period, now)

		return True

	def __getitem__(self, key):
		return super(ITimeCollectible, self).__getitem__(key)

	def __setitem__(self, key, data):
		self.time[key] = int(time())
		return super(ITimeCollectible, self).__setitem__(key, data)

	def __str__(self):
		s = ''

		for k,v in self.iteritems():
			s = s + '%s: %s (%s), ' % (repr(k), repr(v), self.time[k])

		return '{' + s[:-2] + '}'

class TimedDict(ITimeCollectible):
	def timeout(self, k):
		self.forget(k)

if __name__ == '__main__':
	f = TimedDict(5, 2)

	f['asd'] = 'dsa'
	f['dsa'] = 'asd'

	print 'asd' in f

	print f['asd']
	print f

	reactor.run()
