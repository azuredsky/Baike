#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
-------------------------------------------------
   File Name:post_process.py
   Author:jasonhaven
   date:19-1-12
-------------------------------------------------
   Change Activity:19-1-12:
-------------------------------------------------
"""
import json
import os


def main():
	count=0
	for dirpath, dirnames, filenames in os.walk('./data'):
		for fname in sorted(filenames):
			fpath = dirpath + os.path.sep + fname
			if os.path.getsize(fpath) == 0:
				continue
			try:
				with open(fpath, 'r', encoding='utf-8') as f:
					data = json.load(f)
					count=count+1
					print('{}:'.format(count),data)
			except Exception as e:
				print("Error in load file = {}".format(fpath))
				continue

if __name__ == '__main__':
	main()
