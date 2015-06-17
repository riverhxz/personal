#!/usr/bin/python
#-*- coding:utf-8 -*-
import logging
import json,bsddb
import math
import sys
import yaml

#result= json.loads(sys.stdin.read())
#data = result["data"]

'''
通过历史行为和本地帖子库，生成帖子tag
'''

class tag_facotry:
	def __init__(self,db_file,scatter_fields):
		self.conf = yaml.load(open('../conf/fang_tag_gen.yaml').read())
		self.scatter_fields = scatter_fields
		self.logger = logging.getLogger('fang_app_rec')
		self.dbenv = bsddb.db.DBEnv()
		self.dbenv.open('db', bsddb.db.DB_CREATE |bsddb.db.DB_INIT_CDB| bsddb.db.DB_INIT_MPOOL)

		self.db = bsddb.db.DB(self.dbenv)
		self.db.open(db_file,bsddb.db.DB_HASH,bsddb.db.DB_RDONLY,0660)

	def process(self,puids):
		tags_of_posts = []
		trim = len(puids) - 1
		while puids[trim] == '\n':
			trim -= 1
#print puids[:trim].split(',')
		for puid in puids[:trim+1].split(','):
			valid_tags = []
			
			if puid in self.db:
				price_tag=""
				post = json.loads(self.db[puid])
				loc=""
				if "district_id" in post and post["district_id"] != "-1":
					loc="district_id:%s" % post["district_id"]
					if "street_id" in post and post["street_id"] != "-1":
						loc = loc + "&street_id:%s" % post["street_id"]
					valid_tags.append(loc)
#				if "price" in post:
#	self.logger.debug(post["price"])
				price = "price" in post and int(math.log(float(post["price"])+1)/math.log(2)) or 0
				if price > 0:
					price_tag = "l2_price:%d" % price
					valid_tags.append(price_tag)

				area = "area" in post and int(post["price"])/20 or 0
				if area > 0:
					valid_tags.append("area:%s" % area)

				for field in self.scatter_fields.split(','):
					if field in post and post[field] != "-1":
						valid_tags.append("%s:%s" % (field,post[field]))
				if price > 0 and loc and "city" in post and "major_category" in post:
					valid_tags.append("city:%s&major_category:%s&%s&%s" % (post["city"],post["major_category"],loc,price_tag))
				tags_of_posts.append(' '.join(valid_tags))
				

		return tags_of_posts

#if __name__ == '__main__':
#	tg = tag_facotry('tmpdb',"huxing_shi,zhuangxiu,major_category")
#	print tg.process(sys.stdin.read())
