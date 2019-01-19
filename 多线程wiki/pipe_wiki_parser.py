#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
-------------------------------------------------
   File Name:en_wiki_parser.py
   Author:jasonhaven
   date:19-1-12
-------------------------------------------------
   Change Activity:19-1-12:
-------------------------------------------------
"""
from bs4 import BeautifulSoup
from urllib import request
from queue import Queue
import os
import requests
import log
import time
import codecs
import json
import threading
import random
import copy
import datetime

logger = log.Logger().get_logger()
en_base_url = 'https://en.wikipedia.org/wiki/'

properties = ['Preceding', 'Parent ', 'Child ']  # 可以扩展机构的属性


def run_time(func):
	def wrapper(*args, **kw):
		start = datetime.datetime.now()
		func(*args, **kw)
		end = datetime.datetime.now()
		logger.info('finished in {}s'.format(end - start))

	return wrapper


def download_html(url, retry):
	'''
	下载网页

	:param url:
	:param retry:
	:return:
	'''
	try:
		proxy_list = [p.strip() for p in open('proxies.txt').readlines()]
		proxy = random.choice(proxy_list)
		proxy_handler = request.ProxyHandler({'http': proxy})
		opener = request.build_opener(proxy_handler)
		request.install_opener(opener)
		html_doc = requests.get(url, timeout=15).text
		return html_doc
	except Exception as e:
		logger.error("failed and retry = {} to download url {}".format(retry, url))
		if retry >= 0:
			time.sleep(1)
			return download_html(url, retry - 1)


def load_keywords(batch):
	"""
	加载批量关键词
	:param batch:批量关键词列表
	:return:批量关键词
	"""
	keywords = []

	for fp in batch:
		with codecs.open(fp, 'r', encoding='utf-8') as f:
			keywords.extend(list(key.strip() for key in f.readlines() if key.strip() != ''))

	return keywords


def list_files(dir):
	"""
	返回指定目录下全部文件
	:param dir:目录
	:return:
	"""
	return [dir + os.path.sep + f for f in os.listdir(dir)]


def barch_process(keyword_dir='keyword', batch_size=4):
	"""
	批量处理关键字
	:param keyword_dir:关键字目录
	:param data_dir: 存放数据目录
	:param batch_size: 批处理关键字文件数目
	:return:
	"""
	files = []
	batch_files = []

	for f in os.listdir(keyword_dir):
		f = keyword_dir + os.path.sep + f
		if os.path.isdir(f):
			files.extend(list_files(f))
		else:
			files.append(f)

	logger.info("关键词文件总数为:{}".format(len(files)))
	idx = list(range(0, len(files), batch_size))

	for i in range(len(idx) - 1):
		batch_files.append(files[idx[i]:idx[i + 1]])

	batch_files.append(files[idx[-1]:])

	for i, batch in enumerate(batch_files):
		th = ThreadBatch(str(i), batch)  # 在batch级别使用多线程
		th.start()
		time.sleep(1)


class ThreadBatch(threading.Thread):
	def __init__(self, name, batch):
		threading.Thread.__init__(self)
		self.name = name
		self.batch = batch

	@run_time
	def run(self):
		logger.info("create thread for batch's level: {}".format(self.name))
		extract_batch(self.batch)


def extract_batch(batch):
	"""
	批量抽取
	:param batch: 批处理关键字文件
	:return:
	"""
	# keywords = list(set(load_keywords(batch)))
	keywords = ["United States Department of Defense"]
	count = 0
	try:
		for k in keywords:
			count = count + 1
			logger.info('{}/{}...'.format(count, len(keywords)))

			fpath = 'data/enwiki_{}.json'.format(k)

			if os.path.exists(fpath):  # 如果已经存在结果文件则跳过
				continue

			infobox, related_keywords = extract_infobox(k)

			if related_keywords:
				keywords.extend(related_keywords)
				keywords = list(set(keywords))

			if infobox == {}:
				if int(random.random() * 10) > 6:  # 以一定概率将失败的关键词重新加入队列
					keywords.append(k)
				keywords = list(set(keywords))
				continue

			keys = []
			for prop in properties:
				keys.extend([key for key in list(infobox.keys()) if key.find(prop) != -1])

			if keys:
				for k in keys:
					keywords.extend(infobox[k].split('\t'))
			keywords = list(set(keywords))
			fp = codecs.open(fpath, 'w', encoding='utf-8')
			json.dump(infobox, fp)
			fp.close()

			time.sleep(2)

	except Exception as e:
		logger.error('Exception in extract_batch {}'.format(batch))


def extract_infobox(keyword):
	"""
	抽取指定URL的infobox
	:param keyword:
	:return:infobox json
	"""
	infobox = {}
	related_keywords = []
	infobox['Name'] = keyword

	try:
		url = en_base_url + keyword.strip().replace(' ', '_')
		html = download_html(url, 3)

		related_keywords = extract_related_keywords(keyword, copy.deepcopy(html))  # 扩展相关机构

		soup = BeautifulSoup(html, 'lxml')
		table = soup.find('table', class_="infobox")

		try:
			trs = table.find_all('tr')
		except:
			raise RuntimeError

		for i in trs:
			try:
				tag = i.find('th').getText()
				tag = tag.replace('\n', '\t')
			except:
				tag = "None"
			try:
				val = i.find('td').getText()
				div = i.find('div', class_="plainlist")

				if div is not None:
					val = '\t'.join([x.text for x in div.find_all('li')])
				else:
					val = val.replace('\n', '\t')
			except:
				val = 'None'

			infobox[tag] = val
	except RuntimeError as e1:
		logger.error("Keyword = '{}' RuntimeError.".format(keyword))
		return {}, related_keywords
	return infobox, related_keywords


def extract_related_keywords(keyword, html):
	"""
	抽取指定URL的nowraplinks
	:param keyword:
	:param html:
	:return:list
	"""
	related_keywords = []
	try:
		# url = en_base_url + keyword.strip().replace(' ', '_')
		# html = download_html(url, 3)
		soup = BeautifulSoup(html, 'lxml')
		for table in soup.find_all('table', class_="nowraplinks navbox-subgroup"):
			for link in table.find_all('li'):
				for a in link.find_all('a'):
					k = a.get('title')
					if k is None or k == '' or k.startswith('List'):
						continue
					related_keywords.append(k)
		return related_keywords
	except RuntimeError as e1:
		logger.error("Keyword = '{}' RuntimeError.".format(keyword))
		return []


if __name__ == '__main__':
	# barch_process(keyword_dir='keyword', batch_size=2)
	extract_batch(1)
# extract_related_keywords('United States Department of Defense','')
