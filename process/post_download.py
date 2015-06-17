#!/usr/bin/python
#-*- coding:utf-8 -*-
import pycurl,hashlib,time,StringIO,json
import bsddb
import sys

from env import *
'''
从帖子库获取帖子，存入本地缓存
目前使用bsddb作为本地缓存

一次请求最多获取50个帖子，对于一个用户只去最后五十个历史帖子

'''
class post_download:

	def __init__(self,db_file,fields):
		self.UID='104'
		self.PASSWD='ganjituijian_!)@&_20141126'
		self.c = pycurl.Curl()
		
		file_path='db'
		
		self.dbenv = bsddb.db.DBEnv()
		self.dbenv.open(file_path, bsddb.db.DB_CREATE |bsddb.db.DB_INIT_CDB| bsddb.db.DB_INIT_MPOOL)

		self.db = bsddb.db.DB(self.dbenv)
		self.db.open(db_file,bsddb.db.DB_HASH,bsddb.db.DB_CREATE,0660)

		field_set = set(fields)
		if "" in field_set: 
			field_set.remove("")
		field_list = sorted(field_set)
		self.fields = ",".join(field_list) 

	def __del__(self):
		self.db.close()
		self.dbenv.close()
	
	def process(self,puid):
			
			puid_list = puid[:-1].split(',')
			start = len(puid_list)>50 and len(puid_list)-50 or 0
			checked_puid = []
			if("fields" in self.db and self.db["fields"] == self.fields):
				for item in puid_list[start:]:
					if item not in self.db:
						checked_puid.append(item)

			else:
				self.db["fields"] = self.fields
				for item in puid_list[start:]:
					checked_puid.append(item)
			
			if checked_puid:
				now=int(time.time())
				buff = StringIO.StringIO()
				key=hashlib.md5("%s%s%s" % (self.UID,self.PASSWD,now)).hexdigest()
				data='puid=%s&fields=%s' % (','.join(checked_puid),self.fields)
				URL="http://tg.dns.ganji.com/api.php?c=FangQuery&a=getInfoByPuid&userid=%s&time=%d&key=%s" % (self.UID,now,key)
				self.c.setopt(pycurl.URL, URL)
				self.c.setopt(pycurl.WRITEFUNCTION, buff.write)
				self.c.setopt(pycurl.POST, 1)
				self.c.setopt(pycurl.POSTFIELDS, data)
				self.c.perform()
				posts = json.loads(buff.getvalue().encode())["data"]
				logger.debug("download %d posts",len(posts))
				for one_puid in posts:
					self.db[one_puid.encode()] =json.dumps(posts[one_puid])



#if __name__ == '__main__':
#	fields='puid,city,district_id,price,huxing_shi,tab_system,zhuangxiu,street_id,major_category,ceng,chaoxiang,image_count,fang_xing,xiaoqu_id,huxing_ting,huxing_wei,peizhi'
#	pd = post_download('tmpdb',fields,False)
#	pd.process(sys.stdin.read())
