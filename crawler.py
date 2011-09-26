#!/usr/bin/python

from multiprocessing import *
from urlparse import urlparse
from urllib import urlopen
from pymongo import *
import htmlentitydefs
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

##
# Removes HTML or XML character references and entities from a text string.
# By: Fredrik Lundh on 10/28/2006 
# @param text The HTML (or XML) source text.
# @return The plain text, as a Unicode string, if necessary.

def unescape(text):
    def fixup(m):
        text = m.group(0)
        if text[:2] == "&#":
            # character reference
            try:
                if text[:3] == "&#x":
                    return unichr(int(text[3:-1], 16))
                else:
                    return unichr(int(text[2:-1]))
            except ValueError:
                pass
        else:
            # named entity
            try:
                text = unichr(htmlentitydefs.name2codepoint[text[1:-1]])
            except KeyError:
                pass
        return text # leave as is
    return re.sub("&#?\w+", fixup, text)

def site(url):
	return {'_url':url, 
			'_page':unescape(urlopen(url).read())
			}

def link(to, frm):
	return {'_to':to, '_from':frm}

def visit(l):

	con=Connection()
	pages=con.crawldb.pages
	links=con.crawldb.links
	try:
		if pages.find_one({'_url':l['_to']}):
			pages.update({'_url':l['_to']}, {'$addToSet': {'_inbound':l['_from']}})

		else:
			s=site(l['_to'])

			pagelinks=linkfinder.findall(s['_page'])
			s['_outbound']=filter(lambda x: x != l['_to'], pagelinks)
			if l['_from']: s['_inbound']=[l['_from']];

			if s['_outbound']:
				links.insert([link(o, l['_to']) for o in s['_outbound']])
			pages.insert(s)
		return True

	except Exception as e:
		if not links.find_one(l):
			links.insert(l)

def elapsed(s):
	return time.time()-s

def visitpage(pool, l):
	return pool.apply_async(visit, (l,))

def crawl(start=time.time()):

	con=Connection()
	pages=con.crawldb.pages
	lastcount=startcount=pages.count()
	links=con.crawldb.links

	results=[]
	newpages=0

	pool=Pool(processes=MaxChildren)

	while (newpages < MinPages) and (elapsed(start) < TimeLimit):

		if links.count() > 0:
			l=links.find_one()
			links.remove(l)
			results.append((l, visitpage(pool, l)))

		while len(results) >= MaxChildren:
			i=0
			while i < len(results) and len(results) > 0: 
				r=results[i][1]
				r.wait(0.01)
				if r.ready(): 
					results.pop(i)
				else: i+=1

		if pages.count() > lastcount:
			lastcount=pages.count()
			newpages=lastcount-startcount

	while results:
		l, r=results.pop(0)
		if not r.ready(): 
			if not links.find_one(l):
				links.insert(l)

	print 'done crawling'
	pool.terminate()
	pool.join()
	print 'done cleanup'
	con.disconnect()

	print "crawled {0} pages in {1} seconds".format(newpages, elapsed(start))
	print "the database now contains {0} sites".format(pages.count())

if __name__ == '__main__':
	crawl()
