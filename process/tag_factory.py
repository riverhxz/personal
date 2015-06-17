#!/usr/bin/python
#-*- coding:utf-8 -*-
import logging
import json,bsddb
import math
import sys
import yaml
import re

#result= json.loads(sys.stdin.read())
#data = result["data"]

'''
通过历史行为和本地帖子库，生成帖子tag
'''

class tag_facotry:
	def __init__(self,db_file,conf):
#self.conf = yaml.load(open('../conf/fang_tag_gen.yaml').read())
		self.conf = conf
		self.logger = logging.getLogger('fang_app_rec')
		self.dbenv = bsddb.db.DBEnv()
		self.dbenv.open('db', bsddb.db.DB_CREATE |bsddb.db.DB_INIT_CDB| bsddb.db.DB_INIT_MPOOL)
		self.PUID_PATTERN=re.compile('[0-9]+')

		self.db = bsddb.db.DB(self.dbenv)
		self.db.open(db_file,bsddb.db.DB_HASH,bsddb.db.DB_RDONLY,0660)

	def process_pn(self,pos_samples,neg_samples):
		pos_tags=self.process(pos_samples)
		pos_set = set(pos_samples[:-1].split(','))
		filterd_neg_set = set()
		for neg in neg_samples[:-1].split(','):
			if neg not in pos_set:
				filterd_neg_set.add(neg)
		neg_tags=self.process(','.join(filterd_neg_set))

		return (pos_tags,neg_tags)


	def process(self,puids):
#处理基于内容特征
		tags_of_posts = []
		for puid in puids[:-1].split(','):
			valid_tags = []
			if puid in self.db:
				post = json.loads(self.db[puid])

				for (k,v) in self.conf['convert_real_log'].items():
					if k in post:
						val = float(post[k])
						if val > 0 and math.log(val) > 0:
							post[v]=str(int(math.log(val)/math.log(self.conf["constants"]['base_of_log'])))

				for (k,v) in self.conf['convert_real_div'].items():
					if k in post:
						val = float(post[k])
						if val > 0 and math.log(val) > 0:
							post[v]=str(int(math.log(val)/math.log(self.conf["constants"]['divider'])))

				for (k,v) in self.conf['convert'].items():
					if k in post:
						post[v] = post[k]

				for item in self.conf['split']:
					if item in post and post[item]:
						for x in post[item].split(','):
							if x:
								valid_tags.append("%s:%s" % (item,x))

				for field in self.conf['scatter']:
					if field in post and '-1' != post[field]:
						valid_tags.append("%s:%s" % (field,post[field]))	

				for combo in self.conf['combo']:
					values = []
					complete = 1
					for field in combo:
						if field in post and '-1' != post[field]:
							values.append(post[field])
						else:
							complete = 0
							break
					if complete:
						valid_tags.append('&'.join(["%s:%s" % (x,y) for (x,y) in zip(combo,values)]))

				tags_of_posts.append(' '.join(valid_tags))

		return tags_of_posts

	def process_by_puid(self,puid_file):
#处理协同过滤特征
		for line in open(puid_file,'rU'):
			valid_tags=[]
			puid = line[:-1]
			if not puid in self.db:
				continue
			post = json.loads(self.db[puid])
			if "city" in post and post["city"] != "0":
				continue
			for (k,v) in self.conf['convert_real_log'].items():
				if k in post:
					try:
						val = float(post[k])
					except TypeError:
						break
					if val > 0 and math.log(val) > 0:
						post[v]=str(int(math.log(val)/math.log(self.conf["constants"]['base_of_log'])))

			for (k,v) in self.conf['convert_real_div'].items():
				if k in post:
					try:
						val = float(post[k])
					except TypeError:
						break
					if val > 0 and math.log(val) > 0:
						post[v]=str(int(math.log(val)/math.log(self.conf["constants"]['divider'])))

			for item in self.conf['split']:
				if item in post and post[item]:
					for x in post[item].split(','):
						if x:
							valid_tags.append("%s:%s" % (item,x))

			for field in self.conf['scatter']:
				if field in post and '-1' != post[field]:
					valid_tags.append("%s:%s" % (field,post[field]))
			for combo in self.conf['combo']:
				values = []
				complete = 1
				for field in combo:
					if field in post and '-1' != post[field]:
						values.append(post[field])
					else:
#	print "%s not in post" % (field)
						complete = 0
						break
				if complete:
					valid_tags.append('&'.join(["%s:%s" % (x,y) for (x,y) in zip(combo,values)]))
#self.buff['puid'] = pickle.dump(valid_tags)
			print '%s\t%s' % (puid,' '.join(valid_tags))
			

	def process_by_city(self,puids,uuid):
		tags_of_posts = []
		trim = len(puids) - 1
		while puids[trim] == '\n':
			trim -= 1
#print puids[:trim].split(',')
		for puid in puids[:trim+1].split(','):
			valid_tags = []
			city_count={}
			if puid in self.db:
				post = json.loads(self.db[puid])
				if "city" in post:
					city_count[post["city"]] = post["city"] in city_count and city_count[post["city"]]+1 or 1

		if city_count:
			big_city=city_count.keys()[0];
			for x in city_count:
					if city_count[x] > city_count[big_city]:
						big_city = x
			print "%s\t%s" % (big_city,uuid)

		return tags_of_posts
