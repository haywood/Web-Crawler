#!/usr/bin/python

from htmlentitydefs import entitydefs
from multiprocessing import *
from pymongo.code import Code
from urlparse import urlparse
from urllib import urlopen
from pymongo import *
import time
import sys
import re

lfind=re.compile(r"<a href=.?(http://[^ >'\"]+)[^>]*>").findall
tkill=re.compile(r"<!?--.*?-->|<(style|(no)?script).*?>.*?</\1>|<.*?>|\s+", flags=re.S).sub
ekill=re.compile(r"&[a-z0-9]+?;").sub

entitydefs['apos']="'"

def entityrepl(match):
	text=match.group(0);
	hex=re.search('x[a-f\d]+', text)
	dec=re.search('\d+', text)
	c=' '
	if hex: c=unichr(int(hex.group(0), 16))
	elif dec: c=unichr(int(dec.group(0)))
	else: 
		if text[1:-1] in entitydefs:
			c=entitydefs[text[1:-1]]
		else:
			print 'unrecognized html entity', text
			c=' '
	try: c.encode('utf-8')
	except: c=' '
	return c

def site(url):

	p=urlopen(url).read()
	s={'_url':url, '_visited':False, '_words':False, '_wordsAdded':False}
	s['_page']=unicode(p, 'utf-8').lower()
	return s

def link(to, frm):
	return {'_to':to, '_from':frm}

def successors(s):
	ls=[x for x in lfind(s['_page']) if x != s['_url']]
	s['_outbound']=[{'_url':l, '_visited':False} for l in ls]
	return len(s['_outbound'])

def visit(l, links):

	con=Connection()
	pages=con.crawldb.pages

	if pages.find_one({'_url':l['_to']}) and l['_from']:
		pages.update({'_url':l['_to']}, {'$addToSet': {'_inbound':l['_from']}})

	else:
		try: s=site(l['_to'])
		except (UnicodeDecodeError, IOError) as e:
			print e, l
			return

		if l['_from']: 
			s['_inbound']=[l['_from']]
		successors(s)
		s['_page']=tkill(' ', s['_page'])
		try:
			s['_page']=ekill(entityrepl, s['_page'])
		except Exception as e:
			print e, l
			return
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
		with open('links') as f:
			for line in f: links.append(link(line.strip(), ''))

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
	n=len(links)
	i=0

	while (newpages < n) and (elapsed(start) < TimeLimit):

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
