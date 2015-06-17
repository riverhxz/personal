#!/usr/bin/python
#-*- coding:utf-8 -*-
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
		self.validator = re.compile('.+?\t[0-1]\t.+')
		self.redis_key_rank_format = 'fang:rank_model:%s'
		self.redis_key_recall_format = 'fang:recall_model:%s'
		self.ttl = 60*60*24*30


	def process_np(self):
		
		last_uuid = ''
		last_label = 0
		last_history = ''
		for line in sys.stdin:
			valid = self.validator.match(line)
			if valid:
				(uuid, label, hisorty) = line[:-1].split('\t')
				if label == '0':
					last_uuid = uuid
					last_label = label
					last_history = hisorty
					continue
				elif label == '1':
					if last_uuid == uuid and last_label== '0':
						##下载
						self.pd.process(puid=last_history)
						self.pd.process(puid=hisorty)
						##生成tag
						(pos_post_tags, neg_post_tags) = self.tf.process_pn(pos_samples=hisorty, neg_samples=last_history)
#						print "uuid:",uuid
#						print "positive sample"
#						print '\n'.join(pos_post_tags)
#						print "negtive sample"
#						print '\n'.join(neg_post_tags)
						len_negs = len(neg_post_tags)
						len_poses = len(pos_post_tags)	
						if len_negs < 4 or len_poses < 4:
							continue
#						else:
#	print len_negs, len_poses
						##生成帖子向量
						(tag_mat,label,tag_list) = self.vf.process_pn(pos_sample=pos_post_tags, neg_sample=neg_post_tags)
						(recall_tag_mat,recall_label,recall_tag_list) = self.vf.process_pn(pos_sample=pos_post_tags, neg_sample=[])
						##生成训练测试集
						tt_boundary = 0.8
						len_pos = len(pos_post_tags)
						len_test = len(neg_post_tags)
#print "len_pos,len_test"
#						print len_pos,len_test

						train_pos = tag_mat[: int(tt_boundary * len_pos)]
						test_pos = tag_mat[int(tt_boundary * len_pos):len_pos]

						train_neg = tag_mat[len_pos: len_pos+int(tt_boundary*len_test)]
						test_neg = tag_mat[len_pos+int(tt_boundary*len_test):]

						merge = lambda x,y: [x.append(z) for z in y]
						merge_label = lambda x,y,l: [x.append(l) for z in y]
						train_set = []
						merge(train_set,train_pos)
						merge(train_set,train_neg)
						train_label = []
						merge_label(train_label,train_pos,1)
						merge_label(train_label,train_neg,0)

						test_set = []
						merge(test_set,test_pos)
						merge(test_set,test_neg)
						test_label = []
						merge_label(test_label,test_pos,1)
						merge_label(test_label,test_neg,0)
#
#						print "train_label,len(train_set)"
#						print train_label,len(train_set)
						(pu, r_hat) = self.rd.r_svd_np(train_set, train_label)
#						(recall_pu, recall_r_hat) = self.rd.r_svd_np(recall_tag_mat, recall_label, reg=1)
						recall_pu = self.rd.count(recall_tag_mat)

						pred = self.rd.predict(test_set,pu)
#						print "learn,pred,test_label"
#						print r_hat.T,pred.T,test_label 
						tag_list.append('user_bias:0')
#						recall_tag_list.append('user_bias:0')
						t = zip(test_label, pred.T.tolist()[0])
#print t
						rank = sorted(t, key=lambda x: x[1], reverse=True)
#						print rank
						len_test_pos = len(test_pos)
						tp = 0.0
						fp = 0.0
						fn = 0.0
						tn = 0.0
#						print rank ,len(rank), len_test_pos
						for pos in range(len(rank)):
							if pos < len_test_pos:
								if 1 == rank[pos][0]:
									tp += 1
								elif 0 == rank[pos][0]:
									fp += 1
							else:
								if 1 == rank[pos][0]:
									fn += 1
								elif 0 == rank[pos][0]:
									tn += 1
#						print rank,len_test_pos
#						print tp, fp, fn, tn
#						print uuid, tp/(tp + fp), float(len_poses)/(len_poses + len_negs) , len_poses + len_negs
						
#						user_model = " ".join(["%s#%f" % (x,y) for (x,y) in zip(tag_list,pu.T.tolist()[0])])
						t = dict(zip(tag_list,pu.T.tolist()[0]))
						top_feature = sorted(t,key=lambda x: t[x],reverse=True)
						user_model = ' '.join([ '%s#%f' % (x,t[x]) for x in top_feature]) 
#						print ' '.join([ '%s#%f' % (x,t[x]) for x in top_feature]) 
#						print pu

						t = dict(zip(recall_tag_list,recall_pu.T.tolist()[0]))
						recall_top_feature = sorted(t,key=lambda x: t[x],reverse=True)
						recall_user_model = ' '.join([ '%s#%f' % (x,t[x]) for x in recall_top_feature]) 
#						print recall_user_model

						sent_rank = self.redis_out.setex(self.redis_key_rank_format % uuid, self.ttl, user_model)
						sent_recall = self.redis_out.setex(self.redis_key_recall_format % uuid, self.ttl, recall_user_model)
						if (sent_rank and sent_recall) == 0:
							logger.warning("fail to send result into redis: uuid:%s",uuid)
						else:
							logger.info("sent %s" % uuid)
#print uuid,user_mode
			else: logger.warning("invalid record, input of process: %s.",line)


	def process(self):
		
		last_uuid = ''
		last_label = 0
		last_history = ''
		for line in sys.stdin:
			valid = self.validator.match(line)
			if valid:
				(uuid, label, hisorty) = line[:-1].split('\t')
				if label == '0':
					last_uuid = uuid
					last_label = label
					last_history = hisorty
					continue
				elif label == '1':
					if last_uuid == uuid and last_label== '0':
						##下载
						self.pd.process(puid=last_history)
						self.pd.process(puid=hisorty)
						##生成tag
						(pos_post_tags, neg_post_tags) = self.tf.process_pn(pos_samples=hisorty, neg_samples=last_history)
#						print "uuid:",uuid
#						print "positive sample"
#print '\n'.join(pos_post_tags)
#						print "negtive sample"
#						print '\n'.join(neg_post_tags)
						len_negs = len(neg_post_tags)
						len_poses = len(pos_post_tags)	
						if len_negs < 5 or len_poses < 5:
							continue	
#						else:
#	print len_negs, len_poses
						##生成帖子向量
						(tag_mat,label,tag_list) = self.vf.process_pn(pos_sample=pos_post_tags, neg_sample=neg_post_tags)
						##生成训练测试集
						tt_boundary = 0.5
						len_pos = len(pos_post_tags)
						len_test = len(neg_post_tags)
#print "len_pos,len_test"
#						print len_pos,len_test

						train_pos = tag_mat[: int(tt_boundary * len_pos)]
						test_pos = tag_mat[int(tt_boundary * len_pos):len_pos]

						train_neg = tag_mat[len_pos: len_pos+int(tt_boundary*len_test)]
						test_neg = tag_mat[len_pos+int(tt_boundary*len_test):]

						merge = lambda x,y: [x.append(z) for z in y]
						merge_label = lambda x,y,l: [x.append(l) for z in y]
						train_set = []
						merge(train_set,train_pos)
						merge(train_set,train_neg)
						train_label = []
						merge_label(train_label,train_pos,1)
						merge_label(train_label,train_neg,0)

						test_set = []
						merge(test_set,test_pos)
						merge(test_set,test_neg)
						test_label = []
						merge_label(test_label,test_pos,1)
						merge_label(test_label,test_neg,0)
#
#						print "train_label,len(train_set)"
#						print train_label,len(train_set)
						(pu, r_hat) = self.rd.r_svd_np(train_set, train_label)
						pred = self.rd.predict(test_set,pu)
#						print "learn,pred,test_label"
#						print r_hat.T,pred.T,test_label 
						tag_list.append('user_bias:0')
						t = zip(test_label, pred.T.tolist()[0])
#print t
						rank = sorted(t, key=lambda x: x[1], reverse=True)
#						print rank
						len_test_pos = len(test_pos)
						tp = 0.0
						fp = 0.0
						fn = 0.0
						tn = 0.0
#						print rank ,len(rank), len_test_pos
						for pos in range(len(rank)):
							if pos < len_test_pos:
								if 1 == rank[pos][0]:
									tp += 1
								elif 0 == rank[pos][0]:
									fp += 1
							else:
								if 1 == rank[pos][0]:
									fn += 1
								elif 0 == rank[pos][0]:
									tn += 1
#						print rank,len_test_pos
#						print tp, fp, fn, tn
#						print uuid, tp/(tp + fp), float(len_poses)/(len_poses + len_negs) , len_poses + len_negs
						
#						user_model = " ".join(["%s#%f" % (x,y) for (x,y) in zip(tag_list,pu.T.tolist()[0])])
						t = dict(zip(tag_list,pu.T.tolist()[0]))
						top_feature = sorted(t,key=lambda x: t[x],reverse=True)
						user_model = ' '.join([ '%s#%f' % (x,t[x]) for x in top_feature]) 
#						print ' '.join([ '%s#%f' % (x,t[x]) for x in top_feature]) 
#						print pu

						sent = self.redis_out.setex(self.redis_key_format % uuid, self.ttl, user_model)
						if sent == 0:
							logger.warning("fail to send result into redis: uuid:%s",uuid)
						else:
							logger.debug("sent %s" % uuid)
#print uuid,user_model



#self.pd.process(puid=hisorty)
#post_tags = self.tf.process(puids=hisorty)
#				(tag_mat,tag_list) = self.vf.process(post_tags)
#				pu = self.rd.r_svd(tag_mat)
#				user_model = " ".join(["%s#%f" % (x,y) for (x,y) in zip(tag_list,pu.tolist()[0])])
#				sent = self.redis_out.setex(self.redis_key_format % uuid, self.ttl, user_model)
#				if sent == 0:
#					logger.warning("fail to send result into redis: uuid:%s",uuid)
#				else:
#					logger.debug("sent %s" % uuid)
#	print uuid,user_model
#		else:
#			logger.warning("invalid record, input of process: %s.",record)
		

if __name__ == '__main__':
	p = processer('conf/fang_tag_gen.yaml')
	p.process_np()

