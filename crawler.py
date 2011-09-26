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

def visit(link, links, errlist):

	con=Connection()
	pages=con.crawldb.pages
	try:
		if pages.find_one({'_url':link[1]}):
			pages.update({'_url':link[1]}, {'$addToSet': {'_inbound':link[0]}})

		else:
			s=site(link[1])
			pagelinks=linkfinder.findall(s['_page'])
			s['_outbound']=filter(lambda x: x != link[1], pagelinks)
			links+=[(link[1], l) for l in s['_outbound']]
			if link[0]: s['_inbound']=[link[0]];
			pages.insert(s)
		return True

	except Exception as e:
		errlist.append((time.ctime()+' '+link[1], e))
		return False

def elapsed(s):
	return time.time()-s

def readlinks(links):
	with open('seeds') as f:
		for line in f:
			links.append(eval(line))

	if len(links) == 0:
		print 'error empty seed file'
		sys.exit(1)

def writelinks(links):
	if len(links) > 0:
		with open('seeds', 'w') as f:
			for link in links:
				try: f.write(str(link)+'\n')
				except: pass

def logerrors(errlist):
	if len(errlist) > 0:
		with open('error.log', 'a') as f:
			for e in errlist:
				try: f.write('[{0}] {1}\n'.format(*e))
				except: pass

def visitpage(pool, link, links, errlist):
	return pool.apply_async(visit, (link, links, errlist))

def crawl(start=time.time()):
	con=Connection()
	pages=con.crawldb.pages
	lastcount=startcount=pages.count()
	manager=Manager()
	links=manager.list()
	errlist=manager.list()
	results=[]
	newpages=0

	readlinks(links)

	pool=Pool(processes=MaxChildren)

	while (newpages < MinPages) and (elapsed(start) < TimeLimit):

		if len(links) > 0:
			link=links.pop(0)
			results.append((link, visitpage(pool, link, links, errlist)))

		while len(results) >= MaxChildren:
			i=0
			while i < len(results) and len(results) > 0: 
				l, r=results[i]
				r.wait(0.01)
				if r.ready(): 
					results.pop(i)
					if r.get() == False:
						links.append(l)
				else: i+=1

		if pages.count() > lastcount:
			lastcount=pages.count()
			newpages=lastcount-startcount

	while results:
		l, r=results.pop(0)
		if not r.ready(): 
			links.append(l)

	print 'done crawling'
	pool.terminate()
	pool.join()
	print 'done cleanup'

	writelinks(links)
	logerrors(errlist)

	print "crawled {0} pages in {1} seconds".format(newpages, elapsed(start))
	print "the database now contains {0} sites".format(pages.count())
	con.disconnect()

if __name__ == '__main__':
	crawl()
