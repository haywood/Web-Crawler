#!/usr/bin/python

from multiprocessing import *
from urlparse import urlparse
from urllib import urlopen
from pymongo import *
from pymongo.code import Code
import time
import sys
import re

wordfinder=re.compile("[a-z]+", flags=re.I).finditer
tagkiller=re.compile("(<style.*?</style>)|(<script.*?</script>)|(<noscript.*?</noscript>)|(<.*?>)", flags=re.S).sub

MaxChildren=10
MaxPages=100

def visit(s):
	try:
		con=Connection()
		pages=con.crawldb.pages
		text=tagkiller(' ', s['_page'])
		if text:
			s['_words']={}
			for w in wordfinder(text):
				w=w.group(0).lower()
				if w in s['_words']:
					s['_words'][w]+=1
				else: s['_words'][w]=1

			words=con.crawldb.words
			for w, c in s['_words'].iteritems():
				if words.find_one({'_word':w}):
					words.update({'_word':w}, {'$inc': {'_count': c}})
				else: words.insert({'_word':w, '_count':c})
			pages.save(s)
	except Exception as e:
		print e

if __name__ == '__main__':
	con=Connection()
	pages=con.crawldb.pages

	if pages.count() == 0:
		sys.exit(0)

	extr=pages.find({'_words': {'$ne': False}}).count()
	print 'so far {0}% of the database has had words extracted'.format(100*float(extr)/pages.count())
	pool=Pool(processes=MaxChildren)
	start=time.time()
	results=[]
	for p in pages.find({'_words':False}, limit=MaxPages):

		results.append(pool.apply_async(visit, (p,)))

		if len(results) > MaxChildren:
			j=0
			while j < len(results):
				results[j].wait(0.01)
				if results[j].ready():
					results.pop(j)
				else: j+=1

	pool.close()
	pool.join()
	extr=pages.find({'_words': {'$ne': False}}).count()
	print 'now {0}% of the database has had words extracted'.format(100*float(extr)/pages.count())
	print 'took {0} seconds'.format(time.time()-start)
