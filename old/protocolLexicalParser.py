# -*- coding: utf-8 -*-

import re

class Transition:
	def __init__(self, nextState, regex, callback):
		try:
			self.regex = re.compile(regex)
		except Exception as e:
			print regex
			print e
			print "Regular expression compile unsecessful!"

		self.cback = callback
		self.nextState = nextState

		self.bootStrapFunctions = [ ]
		self.pootStrapFunctions = [ ]

	def match(self, data):
		return self.regex.match(data)

	def callback(self, data):
		return self.cback(data)

	def getNextState(self):
		return self.nextState

class State:
	def __init__(self, name):
		self.name = name
		self.transitions = [ ]

	def parse(self, lParser, data):
		for t in self.transitions:
			if t.match(data):
				lParser.setState(t.getNextState())
				return t.callback(data)

		else:
			raise SyntaxError

	def addTransition(self, nextState, regex, callback):
		self.transitions.append(Transition(nextState, regex, callback))

class LexicalParserFactory:
	def __init__(self, initialState = ''):
		self.states = { }
		self.initialState = initialState
		self.addState('start')

	def addState(self, state):
		if state not in self.states:
			self.states[state] = State(state)
			return self.states[state]
		else:
			raise StateAlreadyInSetError

	def setState(self, state):
		if state in self.states:
			self.state = state
		else:
			raise StateNotInSetError

	def getState(self, state):
		if state in self.states:
			return self.states[state]
		else:
			raise StateNotInSetError

	def getAParser(self):
		return LexicalParser(self.states, self.initialState)

class LexicalParser:
	def __init__(self, states, initialState):
		self.states = states
		self.state = initialState

	def parseData(self, data):
		try:
			r = self.states[self.state].parse(self, data)

			if r:
				return r

		except SyntaxError:
			raise SyntaxError

	def setState(self, state):
		if state in self.states:
			self.state = state
		else:
			raise StateNotInSetError

	def getState(self, state):
		if state in self.states:
			return self.states[state]
		else:
			raise StateNotInSetError

# # # # # # # # # # # # # # #
# A basic protocol example  #
# # # # # # # # # # # # # # #
if __name__ == '__main__':
	from sys import argv

	lParserFactory = LexicalParserFactory()

	def scback(data):
		return tuple(data.split(':'))

	def bcback(data):
		return tuple(data.split(':'))

	def pcback(data):
		return tuple(data.split(':'))

	lParserFactory.addState('start')
	lParserFactory.addState('established')
	lParserFactory.addState('finished')

	(lParserFactory.
			getState('start').
				addTransition('established','c:(a|n):[a-zA-Z]+',scback))

	(lParserFactory.
			getState('established').
				addTransition('finished', 'b:(o|e):[a-zA-Z]+', bcback))

	(lParserFactory.
			getState('established').
				addTransition('established','p:(n|u):[0-9]+\.[0-9]+', pcback))

	lParser = lParserFactory.getAParser()
	lParser.setState('start')

	if len(argv) > 1:
		for i in argv[1:]:
			print lParser.parseData(i)
	else:
		print lParser.parseData('c:a:alderan')
		print lParser.parseData('p:n:9999.999')
		print lParser.parseData('b:e:aaaa')
