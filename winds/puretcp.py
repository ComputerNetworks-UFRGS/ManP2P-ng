# -*- coding: utf-8 -*-

from wind import AbstractTCPWind
from wind import TCPWindFactory

class PureTCPProto(AbstractTCPWind):
	@staticmethod
	def getFactoryClass():
		return PureTCPFactory

	def doDecode(self, data):
		return data

	def doSend(self, data):
		self.transport.write(data)

class PureTCPFactory(TCPWindFactory):
	protocol = PureTCPProto
