# -*- coding: utf-8 -*-

from os import putenv, getenv, listdir
from sys import stderr

from commandLineParser import CommandLineParser

errwrite = stderr.write

class ExtensionLoader:
	class __ExtensionLoaderI:
		def __init__(self, lParserFactory, sParserFactory):
			if lParserFactory is None or sParserFactory is None:
				raise SyntaxError

			self.extensions = [ ]
			self.lParserFactory = lParserFactory
			self.sParserFactory = sParserFactory

			CommandLineParser().add_argument('-e', '--exclude-extensions',
												dest='ntl', nargs='+',
												default=[ ], metavar='EXT',
												help='Extensions not to load')

		def loadExtensions(self, extdir):
			print '-+- Loading Extensions -+-'

			ldir = listdir(extdir)
			ldir.sort()

			for i in ldir:
				if i[-3:] == '.py':
					try:
						print "Reading file: " + i

						ext = __import__(i[3:-3])

						if 'extensionName' not in dir(ext):
							ext.extensionName()

						elif 'extendProtocol' not in dir(ext):
							ext.extendProtocol()

						if 'extendCommandLine' in dir(ext):
							ext.extendCommandLine(self.lParserFactory,
													self.sParserFactory)

						self.addExtension(ext.extensionName(), i, ext)

					except ImportError as e:
						errwrite('Cannot load module ' + i + '\n')
						errwrite(str(e) + '\n')

					except AttributeError as e:
						errwrite('Required methods not found on module ' +
								 i + '\n')
						errwrite(str(e) + '\n')

			print ''

		def extendProtocol(self):
			print "-+- Extending Protocol -+-"

			ntl = CommandLineParser().getArguments().ntl
			
			for i in self.extensions:
				if (i[0] not in ntl and
						i[2].extendProtocol(self.lParserFactory,
											self.sParserFactory)):
					print i[0] + ' extension loaded'
				else:
					self.extensions.remove(i)

			print ''

		def addExtension(self, extensionName, extensionsFile, extension):
			self.extensions.append((extensionName, extensionsFile, extension))

		def getExtension(self, ext):
			for e in self.extensions:
				if e[0] == ext:
					return e[2]
			else:
				raise NoSuchAExtension

		def isActive(self, extensionName):
			if extensionName in map(lambda t: t[0], self.extensions):
				return True

			return False

	# Singleton control block
	__instance = None

	def __init__(self, lParserFactory=None, sParserFactory=None):
		if ExtensionLoader.__instance is None:
			ExtensionLoader.__instance = ExtensionLoader.__ExtensionLoaderI(
												lParserFactory, sParserFactory)

		self.__dict__['_ExtensionLoader__instance'] = ExtensionLoader.__instance

	def __getattr__(self, attr):
		return getattr(self.__instance, attr)

	def __setattr__(self, attr, value):
		return setattr(self.__instance, attr, value)
	# End of Singleton control block

if __name__ == '__main__':
	s = ExtensionLoader([4,2,1,0])
	a = ExtensionLoader()

	s.printIt()
	a.printIt()
