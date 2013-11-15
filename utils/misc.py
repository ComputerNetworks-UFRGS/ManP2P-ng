# -*- coding: utf-8 -*-

class IAnnotable(object):
	annotations = { }

	def addAnnotation(self, annotation, value):
		self.annotations[annotation] = value
		return self

	def getAnnotation(self, annotation):
		return self.annotations[annotation]

	def __getattr__(self, attr):
		try:
			return self.__dict__['annotations'][attr]
		except KeyError:
			raise AttributeError, attr

	def __setattr__(self, attr, value):
		shallWeAnnotate = (
			(attr not in self.__dict__) and
			('annotations' in self.__dict__) and
			(attr in self.annotations)
		)

		if shallWeAnnotate is True:
			self.annotations[attr] = value

		self.__dict__[attr] = value