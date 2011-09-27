#!/usr/bin/python

from multiprocessing import *
from urlparse import urlparse
from urllib import urlopen
from pymongo import *
import pickle
import time
import sys
import re

linkfinder=re.compile("<a href=.?(http://[^ >'\"]+)[^>]*>", flags=re.I)
wordfinder=re.compile("([a-z]+)('[a-z])?", flags=re.I)
tagkiller=re.compile("(<style.*?</style>)|(<script.*?</script>)|(<noscript.*?</noscript>)|(<.*?>)", flags=re.S)

if len(sys.argv) < 4 or len(sys.argv) > 4:
	print 'usage: {0} pages children timelimit'.format(sys.argv[0])
	sys.exit(0)

MinPages=int(sys.argv[1])
MaxChildren=int(sys.argv[2])
TimeLimit=int(sys.argv[3])

def site(url):
	return {'_url':url, '_page':unicode(urlopen(url).read(), 'utf-8')}

def link(to, frm):
	return {'_to':to, '_from':frm}

def successors(s):
	pagelinks=linkfinder.findall(s['_page'])
	s['_outbound']=[x for x in pagelinks if x != s['_url']]
	return len(s['_outbound'])

def visit(l, links):

	con=Connection()
	pages=con.crawldb.pages

	try:
		if pages.find_one({'_url':l['_to']}):
			pages.update({'_url':l['_to']}, {'$addToSet': {'_inbound':l['_from']}})

		else:
			s=site(l['_to'].encode('utf-8'))

			if l['_from']: s['_inbound']=[l['_from']]
			successors(s)
			pages.insert(s)

			if len(s['_outbound']) > 0:
				links+=[link(o, s['_url']) for o in s['_outbound']]

		return True

	except UnicodeDecodeError as ude:
		pass
		#print ude

	except Exception as e:
		pass

def elapsed(s):
	return time.time()-s

def crawl(start=time.time()):

	manager=Manager()
	con=Connection()
	db=con.crawldb
	pages=db.pages
	links=manager.list()
	lastcount=startcount=pages.count()
	newpages=0
	results=[]

	if db.links.count() == 0:
		raise ValueError('frontier is empty')

	for l in db.links.find():
		links.append(l)
	db.links.remove()

	pool=Pool(processes=MaxChildren)

	while (newpages < MinPages) and (elapsed(start) < TimeLimit):

		if len(links) > 0:
			l=links.pop(0)
			results.append(pool.apply_async(visit, (l, links,)))
		
		if pages.count() > lastcount:
			lastcount=pages.count()
			newpages=lastcount-startcount

	print "crawled {0} pages in {1} seconds".format(newpages, elapsed(start))
	pool.terminate()
	pool.join()
	print 'done cleanup'

	print 'saving links'
	while True:
		try: 
			db.links.insert(links, safe=True)
			break
		except errors.AutoReconnect: pass
	con.disconnect()
	print 'finished saving links'

	print 'the database now contains {0} sites'.format(pages.count())
	print 'the frontier is {1} links'.format(db.links.count())

if __name__ == '__main__':
	crawl()
