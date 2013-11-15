# -*- coding: utf-8 -*-

from twisted.protocols.basic import NetstringReceiver

from wind import AbstractTCPWind, TCPWindFactory


class NetStringProtocol(NetstringReceiver, AbstractTCPWind):
	@staticmethod
	def getFactoryClass():
		return PureTCPFactory

	connectionMade = AbstractTCPWind.connectionMade

	def stringReceived(self, data):
		self.preProcess(data)

	def doDecode(self, data):
		return data

	def doSend(self, data):
		self.sendString(data)

class PureTCPFactory(TCPWindFactory):
	protocol = NetStringProtocol
