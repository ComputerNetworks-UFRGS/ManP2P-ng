# -*- coding: utf-8 -*-

import re

IPRegexNot0Octet = "([1-9][0-9]?|1[0-9][0-9]?|2[0-4][0-9]|25[0-4])"
IPRegex0Octet = "(1?[0-9][0-9]?|2[0-4][0-9]|25[0-4])"

IPHostRegex = ( '(' +IPRegexNot0Octet +
				'\.' + IPRegex0Octet +
				'\.' + IPRegex0Octet +
				'\.' + IPRegexNot0Octet + ')')

HostNameRegex = '([a-zA-Z][-a-zA-Z0-9]+)'
GroupNameRegex = HostNameRegex

IPHost = re.compile(IPHostRegex + '$')
HostName = re.compile(HostNameRegex + '$')
