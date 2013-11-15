# -*- coding: utf-8 -*-
#                                                                           80
from overlay import Overlay

from sys import exit

def main(args=None):

	overlay = Overlay()
	overlay.setupProtocol()
	overlay.startOverlay()

if __name__ == "__main__":
	exit(main())
