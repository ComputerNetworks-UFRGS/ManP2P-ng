ManP2P-ng
=========

# P2P-Based Network Management System

ManP2P-ng is a prototype P2P-based network management (P2PBNM) system. Its main purpose is provide a development framework and platform to ease the implementation of novel P2P management applications by developers and system administrators. 

The ManP2P-ng API enables the development of management components and plug-ins. The API also allows modifications in the overlay basic mechanisms (such as replacing the peer’s membership management or message routing protocol), *e.g.*, for testing purposes. Likewise, the API itself can be extended through the plug-in interface.


# Getting started

In order to get started using the ManP2P-ng you need to create a configuratino file for it. Start off by opening the file manp2p.conf and replace the following variables with your host information:

- **HOSTNAME**: Computer's hostname
- **DOMAIN**: Computer's domain name
- **ADDRESS**: Computer's IP address
- **INTRODUCER_ADDRESS**: Introducer's IP address. The introducer is the host used by peers to get into the overlay. 

After that you can start ManP2P-ng using the following command:

*python main.py -c manp2p.conf*

You can also start several instances of ManP2P-ng by using different configuration files. Just change the parameter "name" of the tag "node" for each different instance.
