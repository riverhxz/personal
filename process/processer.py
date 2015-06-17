#!/usr/bin/python
#
import sys,redis
import post_download, tag_factory, vector_factory,recommender 
import re
import yaml

from env import *

class processer:
	def __init__(self,conf_path):
		self.conf=yaml.load(open(conf_path).read())
		db_file = 'tmp_db'
		self.pd = post_download.post_download(db_file,self.conf['all'])
		self.tf = tag_factory.tag_facotry(db_file,self.conf)
		self.vf = vector_factory.vector_factory()
		self.rd = recommender.recommender()
		self.redis_out = redis.StrictRedis(host="yz-rds-lvs-rec.dns.ganji.com",port=26395,db=0) 
		self.validator = re.compile('.+?\t.+')
		self.redis_key_format= 'fang:user_model:%s'
		self.ttl = 60*60*24*3


	def process(self, record):
		valid = self.validator.match(record)
		if valid:
			(uuid, hisorty) = record.split('\t')
			self.pd.process(puid=hisorty)
			post_tags = self.tf.process(puids=hisorty)
			(tag_mat,tag_list) = self.vf.process(post_tags)
			pu = self.rd.r_svd(tag_mat)
			user_model = " ".join(["%s#%f" % (x,y) for (x,y) in zip(tag_list,pu.tolist()[0])])
			sent = self.redis_out.setex(self.redis_key_format % uuid, self.ttl, user_model)
			if sent == 0:
				logger.warning("fail to send result into redis: uuid:%s",uuid)
			else:
				logger.debug("sent %s" % uuid)
#	print uuid,user_model
		else:
			logger.warning("invalid record, input of process: %s.",record)
		

if __name__ == '__main__':
	p = processer('conf/fang_tag_gen.yaml')
	for line in sys.stdin:
		p.process(line)

