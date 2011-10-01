#!/usr/bin/python

from htmlentitydefs import entitydefs, name2codepoint
from multiprocessing import *
from pymongo.code import Code
from urlparse import urlparse
from urllib import urlopen
from pymongo import *
import time
import sys
import re

lfind=re.compile(r"<a href=.?(http://[^ >'\"]+)[^>]*>", flags=re.I).findall
tkill=re.compile(r"<!?--.*?-->|<(style|(no)?script).*?>.*?</\1>|<.*?>|\s+", flags=re.S).sub
ekill=re.compile(r"&[^&]+?;").sub

def entityrepl(match):
	text=match.group(0);
	try:
		if text[1] == '#':
			if text[2] == 'x': return unichr(int(text[3:-1], 16))
			else: return unichr(int(text[2:-1]))
		elif text[1:-1] in entitydefs:
			u=entitydefs[text[1:-1]]
			if u[:2] == '&#': # if entitydefs says ordinal too high
				c=int(text[1:-1])
				if c > 255: return unichr(c, 16)
				elif c > 127: return unichr(c)
			return u
		return ' '
	except: return ' '

def site(url):
	return {'_url':url, 
				'_page':unicode(urlopen(url).read(), 'utf-8'),
				'_visited':False,
				'_words':False,
				'_wordsAdded':False
			}

def link(to, frm):
	return {'_to':to, '_from':frm}

def successors(s):
	ls=[x for x in lfind(s['_page']) if x != s['_url']]
	s['_outbound']=[{'_url':l, '_visited':False} for l in ls]
	return len(s['_outbound'])

def visit(l, links):

	con=Connection()
	pages=con.crawldb.pages

	try:
		if pages.find_one({'_url':l['_to']}) and l['_from']:
			pages.update({'_url':l['_to']}, {'$addToSet': {'_inbound':l['_from']}})

		else:
			s=site(l['_to'].encode('utf-8'))

			if l['_from']: s['_inbound']=[l['_from']]
			successors(s)
			s['_page']=tkill(' ', s['_page'])
			s['_page']=ekill(entityrepl, s['_page'])
			pages.insert(s)

			if len(s['_outbound']) > 0:
				links+=[link(o['_url'], s['_url']) for o in s['_outbound']]

		if l['_from']:
			frm=pages.find_one({'_url':l['_from']})
			if frm:
				for i in range(len(frm['_outbound'])):
					x=frm['_outbound'][i]
					if i+1 == len(frm['_outbound']):
						frm['_visited']=True
					if x['_url'] == [l['_to']]:
						x['_visited'] = True
						break
				pages.save(frm)

	except Exception as e:
		print e
		pass

def elapsed(s):
	return time.time()-s

def crawl():

	if len(sys.argv) < 4 or len(sys.argv) > 4:
		print 'usage: {0} pages children timelimit'.format(sys.argv[0])
		sys.exit(0)

	MinPages=int(sys.argv[1])
	MaxChildren=int(sys.argv[2])
	TimeLimit=int(sys.argv[3])

	manager=Manager()
	con=Connection()
	db=con.crawldb
	pages=db.pages
	links=manager.list()
	lastcount=startcount=pages.count()
	newpages=0

	if pages.count() == 0:
		links.append(link('http://www.google.com/news', ''))
		links.append(link('http://www.cnn.com/', ''))

	else:
		m=Code("function () {"
					"	for (var i=0; i < this._outbound.length; ++i) {"
					"		x=this._outbound[i];"
					"		if (!x['_visited']) {"
					"			emit(this._url, x['_url']);"
					"			break;"
					"		}"
					"	}"
					"}")
		r=Code("function (key, values) {"
					"	return values[0];"
					"}")
		mr=pages.map_reduce(m, r, {'replace':'__links'}, query={"_visited" : False})
		for r in mr.find(): links.append(link(r['value'], r['_id']))

	pool=Pool(processes=MaxChildren)
	start=time.time()
	results=[]
	i=0

	while (newpages < MinPages) and (elapsed(start) < TimeLimit):

		if len(links) > 0:
			l=links.pop(0)
			i+=1
			results.append(pool.apply_async(visit, (l, links)))

		if len(results) > MinPages:
			j=0
			while j < len(results):
				results[j].wait(0.01)
				if results[j].ready():
					results.pop(j)
				else: j+=1

		if pages.count() > lastcount:
			lastcount=pages.count()
			newpages=lastcount-startcount

	print 'joining'
	pool.terminate()
	pool.join()

	newpages=pages.count()-startcount
	print "crawled {0} pages in {1} seconds with {2} wasted links".format(newpages, elapsed(start), i-newpages)
	print 'the database now contains {0} sites'.format(pages.count())
	con.disconnect()
	print 'exiting...'

if __name__ == '__main__':
	crawl()
