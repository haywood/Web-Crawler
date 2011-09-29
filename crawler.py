#!/usr/bin/python

from multiprocessing import *
from urlparse import urlparse
from urllib import urlopen
from pymongo import *
from pymongo.code import Code
import time
import sys
import re

lfind=re.compile("<a href=.?(http://[^ >'\"]+)[^>]*>", flags=re.I)
wordfinder=re.compile("([a-z]+)('[a-z])?", flags=re.I).finditer
tagkiller=re.compile("(<style.*?</style>)|(<script.*?</script>)|(<noscript.*?</noscript>)|(<.*?>)", flags=re.S).sub

if len(sys.argv) < 4 or len(sys.argv) > 4:
	print 'usage: {0} pages children timelimit'.format(sys.argv[0])
	sys.exit(0)

MinPages=int(sys.argv[1])
MaxChildren=int(sys.argv[2])
TimeLimit=int(sys.argv[3])

def site(url):
	return {'_url':url, 
				'_page':unicode(urlopen(url).read(), 'utf-8'),
				'_visited':False
			}

def link(to, frm):
	return {'_to':to, '_from':frm}

def successors(s):
	ls=[x for x in lfind.findall(s['_page']) if x != s['_url']]
	s['_outbound']=[{'_url':l, '_visited':False} for l in ls]
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
			text=tagkiller(' ', s['_page'])
			if text:
				words=con.crawldb.words
				s['_words']={}
				for w in wordfinder(text):
					if w in s['_words']:
						s['_words'][w]+=1
					else: s['_word'][w]=1

					t=words.find_one({'_word':w})
					if t:
						t['_count']+=1
						words.save(t)
					else: words.insert({'_word':w, '_count':1})
			pages.insert(s)

			if len(s['_outbound']) > 0:
				links+=[link(o['_url'], s['_url']) for o in s['_outbound']]

		frm=pages.find_one({'_url':l['_from']})
		for x in frm['_outbound']:
			if x['_url'] == [l['_to']]:
				x['_visited'] = True
		pages.save(frm)

	except Exception as e:
		pass

def savelink(l):
	links=Connection().crawldb.links
	try:
		if not links.find_one(l):
			links.insert(l)
	except: pass

def elapsed(s):
	return time.time()-s

def crawl():

	manager=Manager()
	con=Connection()
	db=con.crawldb
	pages=db.pages
	links=manager.list()
	lastcount=startcount=pages.count()
	newpages=0

	if pages.count() == 0:
		links.append(link('http://www.cnn.com', ''))

	else:
		m=Code("function () {"
					"	for (var i=0; i < this._outbound.length; ++i) {"
					"		x=this._outbound[i];"
					"		if (i+1 == this._outbound.length) {"
					"			this._visited=true;"
					"		}"
					"		if (!x['_visited']) {"
					"			emit(this._url, x['_url']);"
					"			break;"
					"		}"
					"	}"
					"}")
		r=Code("function (key, values) {"
					"	return values[0];"
					"}")
		res=pages.map_reduce(m, r, {'merge':'links'}, query={"_visited" : False})
		for r in res.find(): 
			links.append(link(r['value'], r['_id']))
			print r['_id'], '->', r['value']

	pool=Pool(processes=MaxChildren)
	start=time.time()

	while (newpages < MinPages) and (elapsed(start) < TimeLimit):

		if len(links) > 0:
			l=links.pop(0)
			pool.apply_async(visit, (l, links))

		if pages.count() > lastcount:
			lastcount=pages.count()
			newpages=lastcount-startcount

	print "crawled {0} pages in {1} seconds".format(newpages, elapsed(start))
	print 'the database now contains {0} sites'.format(pages.count())
	print 'joining'
	pool.terminate()
	pool.join()

	con.disconnect()
	print 'exiting...'

if __name__ == '__main__':
	crawl()
