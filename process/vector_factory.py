#!/usr/local/bin/pypy
# -*- coding: utf-8 -*-
import sys
import math
'''
根据帖子属性生成标签向量
'''
class vector_factory:

	def __init__(self):
		pass
#正样本会排在前面，负样本排在后面
	def process_pn(self,pos_sample, neg_sample):

		tag_pool=set([])
		post_tag_list=[]
		result=[]
		label=[]
		for line in pos_sample: 
			tags = line.split(" ")
			post_tag_list.append(tags)
			[tag_pool.add(x) for x in tags]
			label.append(1)
		for line in neg_sample:
			tags = line.split(" ")
			post_tag_list.append(tags)
			[tag_pool.add(x) for x in tags]
			label.append(0)
		sorted_tag_pool=sorted(tag_pool)

		for post in post_tag_list:
			tags = set(post)
			tmp= [(tag in tags and 1 or 0) for tag in sorted_tag_pool]
			result.append(tmp)
		return (result, label, sorted_tag_pool)

	def process(self,data):
		tag_pool=set([])
		post_tag_list=[]
		result=[]
		for line in data: 
			tags = line.split(" ")
			post_tag_list.append(tags)
			[tag_pool.add(x) for x in tags]
			
		sorted_tag_pool=sorted(tag_pool)
		for post in post_tag_list:
			tags = set(post)
			tmp= [(tag in tags and 1 or 0) for tag in sorted_tag_pool]
			result.append(tmp)
		return (result,sorted_tag_pool)
	
	def process_by_puid(self,argv):
		(b,puid_tag_file, tag_list_filename, puid_list_filename, puid_vector_filename) = argv
		tag_pool = set([])
		puid_tags=open(puid_tag_file,'rU')
		count = 0
		l2 = 1
		puid_list=[]
		for line in puid_tags:
			(puid,tag_str) = line[:-1].split('\t')
			tags = tag_str.split(' ')
			[tag_pool.add(x) for x in tags]
			puid_list.append(puid)
#			if count == l2:
#				print count
#				l2 = l2 << 1
#			count += 1
		print len(tag_pool)
		print len(puid_list)
		puid_tags.close()

		sorted_tag_pool=sorted(tag_pool)
		f = open(tag_list_filename,"w+")
		f.write(" ".join(sorted_tag_pool))
		f.close()
		
		f = open(puid_list_filename,"w+")
		f.write(" ".join(puid_list))
		f.close()

		f = open(puid_vector_filename,"w+")
		series = [str(x) for x in range(len(sorted_tag_pool))]
		pool_pair = zip(sorted_tag_pool,series)
#		print " ".join(puid_list)
#		print pool_pair 
		tag_dict=dict(pool_pair)
		puid_tags=open(puid_tag_file,'rU')
		for line in puid_tags:
			(puid,tag_str) = line[:-1].split('\t')
			tags = tag_str.split(' ')
			tmp = [tag_dict[x] for x in tags]
#tmp= [(tag in tags and "1" or "0") for tag in sorted_tag_pool]

			f.write(' '.join(tmp))
			f.write('\n')

		f.close()

		

#if __name__=='__main__':
#	vf = vector_factory()
#	if len(sys.argv) == 5:
#		vf.process_by_puid(sys.argv)
#	else:
#		print "args: puid_tag_file, tag_list_filename, puid_list_filename, puid_vector_filename" 
