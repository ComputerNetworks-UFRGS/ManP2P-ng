# -*- coding: utf-8 -*-

from extensionLoader import ExtensionLoader

# Bureaucratic stuff
extName = 'DTNHealingDataModel'

def extensionName():
	return extName

def extendProtocol(lFactory, sFactory):
	try:
		if ExtensionLoader().isActive("HTTP-DTN") is False:
			raise KeyError, "HTTP-DTN extension not active"

		dtnExtension = ExtensionLoader().getExtension("HTTP-DTN")

		dtnExtension.bundles.runInteraction(
			lambda txn: txn.executescript('''
            -- Enable foreign key support for the database
            PRAGMA foreign_keys = ON;

            -- Table for storing our peers
            CREATE TABLE IF NOT EXISTS healing_group (
                peer VARCHAR(32),
                join_date INTERGER CHECK (join_date > 0),

                PRIMARY KEY (peer)
            );

            -- Table for storing the healing plans
            CREATE TABLE IF NOT EXISTS healing_plans (
                md5 VARCHAR(32),
                plan TEXT,

                PRIMARY KEY (md5)
            );

            -- Table for storing plan's target data
            CREATE TABLE IF NOT EXISTS healing_targets (
                md5 VARCHAR(32),
                address VARCHAR(40),
                port INTERGER,
                transport VARCHAR(16),

                PRIMARY KEY (md5, address, port, transport),

                FOREIGN KEY (md5) REFERENCES healing_plans(md5)
            );

            -- Table for storing extra data for targets and plans
            CREATE TABLE IF NOT EXISTS healing_data (
                md5 VARCHAR(32),
                address VARCHAR(40),
                port INTERGER,
                transport VARCHAR(16),
                attribute VARCHAR(32),
                data TEXT,

                PRIMARY KEY (md5, address, port, transport, attribute),

                FOREIGN KEY (
                    md5, address, port, transport
                ) REFERENCES healing_targets (
                    md5, address, port, transport
                )
            );
            '''))

	except KeyError as e:
		return False

	return True
