# -*- coding: utf-8 -*-
#                                                                           80

from sys import argv

try:
	from argparse import ArgumentParser
except ImportError:
	errwrite(argv[0] + ": argparse module not found!\n")

class CommandLineParser:
	class __clpI(ArgumentParser):
		def __init__(self, args):
			if args is None:
				args = argv[1:]

			self.args = args
			self.namespace = None

			ArgumentParser.__init__(self, description='Basic overlay services')

			ArgumentParser.add_argument(self, '-v', dest='verbose',
										action='store_true',
										default=False,
										help='Turn verbose mode on')

			ArgumentParser.add_argument(self, '-c', '--connectTo',
										dest='mhost', nargs='+',
										default=[ ], metavar='PEER',
										help='Already running peers to connect')

			ArgumentParser.add_argument(self, '-l', '--listePort',
										dest='lport', type=int,
										nargs='+', default=[ 8001 ],
										help='Ports to listen for connection')

			ArgumentParser.add_argument(self, 'PeerName',
										help='The name of this peer')

		def parse_args(self):
			self.namespace = ArgumentParser.parse_args(self, self.args)

		def getArguments(self):
			return self.namespace

	__instance = None

	def __init__(self, args=None):
		if CommandLineParser.__instance is None:
			CommandLineParser.__instance = CommandLineParser.__clpI(args)

		self.__dict__['_CommandLineParser__instance'] =\
													CommandLineParser.__instance

	def __getattr__(self, attr):
		return getattr(self.__instance, attr)

	def __setattr__(self, attr, value):
		return setattr(self.__instance, attr, value)

if __name__ == '__main__':
	from sys import argv

	ap = CommandLineParser(argv[1:])
	ap.parse_args()
