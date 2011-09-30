#!/usr/bin/python

from multiprocessing import *
from urlparse import urlparse
from urllib import urlopen
from pymongo import *
from pymongo.code import Code
import time
import sys
import re

wordfinder=re.compile("([a-z]+)('[a-z])?", flags=re.I).finditer
tagkiller=re.compile("(<style.*?</style>)|(<script.*?</script>)|(<noscript.*?</noscript>)|(<.*?>)", flags=re.S).sub

def visit(s):
	con=Connection()
	pages=con.crawldb.pages
	text=tagkiller(' ', s['_page'])
	if text:
		words=con.crawldb.words
		s['_words']={}
		for w in wordfinder(text):
			w=w.group(0).lower()
			if w in s['_words']:
				s['_words'][w]+=1
			else: s['_words'][w]=1

			t=words.find_one({'_word':w})
			if t:
				t['_count']+=1
				words.save(t)
			else: words.insert({'_word':w, '_count':1})
	pages.save(s)

if __name__ == '__main__':
	con=Connection()
	pages=con.crawldb.pages

	if pages.count() == 0:
		sys.exit(0)

	pool=Pool(processes=10)
	for p in pages.find({'_words':False}):
		pool.apply_async(visit, (p,))
	pool.close()
	pool.join()
