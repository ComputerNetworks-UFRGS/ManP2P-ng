# -*- coding: utf-8 -*-

from wind import AbstractUDPWind
from wind import UDPWindFactory

class PureUDPProto(AbstractUDPWind):
	@staticmethod
	def getFactoryClass():
		return PureUDPFactory

	def doDecode(self, data):
		return data

	def doSend(self, data):
		self.sendDatagram(data)

class PureUDPFactory(UDPWindFactory):
	protocol = PureUDPProto