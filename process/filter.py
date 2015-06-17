#!/usr/bin/python

import sys

previous_uuid = ""
pset = set()
for line in sys.stdin:
	(uuid,cat,plist) = line[:-1].split('\t')
	if uuid == previous_uuid:
		if len(pset) < 50:
			for puid in plist.split('_'):
				pset.add(puid)
	elif "" != previous_uuid:
		print '%s\t0\t%s' % (previous_uuid, ','.join(pset))
		pset.clear()
		previous_uuid = uuid
	else:
		previous_uuid = uuid


print '%s\t%s' % (previous_uuid, ','.join(pset))
