#!/usr/bin/python
#-*- coding:utf-8 -*- 
import sys
from numpy import mat,ones,zeros 
import numpy as np
from scipy.linalg import norm
from scipy.sparse import *
from numpy import concatenate
import gc
class recommender:
	def predict(self, data, pu):
		a=mat(data)
		u_b = mat(ones((a.shape[0],1)))
		a = concatenate((a, u_b), 1)
		return a*pu

	def count(self, data):
		a=mat(data,dtype=int)
		return sum(a,0).T
		
	def r_svd_np(self,data,label,reg = 0.4):
#根据帖子属性生成标签向量
		a=mat(data)
		u_b = mat(ones((a.shape[0],1)))
		a = concatenate((a, u_b), 1)
		pu = mat(zeros(a.shape[1])).T
		learn_rate = 0.1
		r = mat(label).T
		reg = 0.4
		count = 1
		err_new = 0
		err_old = 1
		gc.disable()
		while(abs((err_new-err_old)/(err_new+err_old))>0.001 and count < 300):
			err_old = err_new
			ind_post = 0
			for post in a:
				err = r[ind_post,0]-post*pu
				pu += learn_rate*((err*post).T-reg*pu)
				ind_post += 1
			m_err = r - a*pu
			err_new = m_err.T*m_err + reg * pu.T*pu
			count = count + 1
			learn_rate = learn_rate * 0.9+0.01
#print "iteration:%d,err:%f" % (count, err_new)

#		print "iteration:%d,err:%f" % (count, err_new)
		t = r - a*pu
#		print pu
		gc.enable()
		return (pu, a * pu)

#已废弃
	def r_svd(self,data):
#根据帖子属性生成标签向量
		a = mat(data)
		row_num = a.shape[1]
		pu = mat(ones(row_num))
		learn_rate = 0.1
		reg = 0.1
		r = 1
		count = 1
		lc = 1
		err_new = 0
		err_old = 1
		gc.disable()
		while(abs(err_new-err_old)>0.000001):
			err_old = err_new
			for post in a:
				err = r-pu*post.T
				pu = pu + learn_rate*(err*post-reg*pu)
				err = (r-a*pu.T)
			err_new = err.T*err
			if(count == lc):
#print "iteration:%d,err:%f" % (count,err_new)
				lc = lc << 1
			if count == 3000: break
			count = count + 1
			learn_rate = learn_rate * 0.9+0.01
#		print "iteration:%d,err:%f" % (count,err_new)
#		print a*pu.T
#		print pu
		gc.enable()
		return pu

	def n_svd(self,argv):
		if len(argv) != 6:
			print "args: post_vector,post_list,user_list,tag_list,pv_filename"
			return
		
		(b,post_vector,post_list,user_list,tag_list,pv_filename) = argv
#加载帖子数据
		post_number = 0
		feature_number = 0
		user_number = 0
		posts_of_user = {}
		users_of_post = {}
		with open(post_list) as f:
			post_number = f.readline()[:-1].count(' ') + 1
		with open(user_list) as f:
			user_number = f.readline()[:-1].count(' ') + 1
		with open(tag_list) as f:
			feature_number = f.readline()[:-1].count(' ') + 1
		
		post = mat(zeros((post_number,feature_number)))
		user = mat(zeros((user_number,feature_number)))

		with open(post_vector, 'rU') as post_file:
			ind_post = 0
			for line in post_file:
				tag_pos = line[:-1].split(" ")
				for pos in tag_pos:
					post[ind_post,pos] = 1
				ind_post += 1

#		print post_number,feature_number,user_number 


#加载pv 文件
#		pv_file = open(pv_filename,'rU')
#第一行user总数
#		user_number = int(pv_file.readline())
#第二行user列表
#		pv_file.read()
#pv

#if __name__ == '__main__':
#	rd = recommender() 
#	rd.n_svd(sys.argv)

