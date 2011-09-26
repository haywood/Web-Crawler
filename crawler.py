#!/usr/bin/python

from multiprocessing import *;
from urlparse import urlparse;
from urllib import urlopen;
from pymongo import *;
import htmlentitydefs;
import pickle;
import time;
import sys;
import re;

linkfinder=re.compile("<a href=.?(http://[^ >'\"]+)[^>]*>", flags=re.I);
wordfinder=re.compile("([a-z]+)('[a-z])?", flags=re.I);
tagkiller=re.compile("(<style.*?</style>)|(<script.*?</script>)|(<noscript.*?</noscript>)|(<.*?>)", flags=re.S);

if len(sys.argv) < 4 or len(sys.argv) > 4:
	print 'usage: {0} pages children timelimit'.format(sys.argv[0]);
	sys.exit(0);

MinPages=int(sys.argv[1]);
MaxChildren=int(sys.argv[2]);
TimeLimit=int(sys.argv[3]);

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
    return re.sub("&#?\w+;", fixup, text)

def site(url):
	return {'_url':url, '_page':unescape(urlopen(url).read())};

def visitpage(con, links, errlist):

	if len(links) == 0:
		return;

	l=links.pop(0);
	try:
		pages=con.crawldb.pages;

		if not pages.find_one({'_url':l}):
			s=site(l);
			pagelinks=linkfinder.findall(s['_page']);
			pages.insert(s);
			links+=filter(lambda x: x != l, pagelinks);

	except Exception as e:
		errlist.append((time.ctime()+' '+l, e));

def crawl(con, links, errlist, start=time.time()):
	count=con.crawldb.pages.count;
	lastcount=startcount=count();
	i=0;
	while ((count() - startcount) < MinPages) and ((time.time() - start) < TimeLimit):
		if len(active_children()) - 1 < MaxChildren:
			Process(name="visitor-"+str(i), target=visitpage, 
						args=(con, links, errlist)).start();
			i+=1;

		if count() > lastcount:
			lastcount=count();
			print 'at {0} new pages'.format(lastcount-startcount);

	print 'done crawling';

	for child in active_children():
		if re.match("visitor", child.name):
			child.terminate();

	print 'done cleanup';
	return count()-startcount;

if __name__ == '__main__':

	manager=Manager();
	links=manager.list();
	errlist=manager.list();

	with open('seeds') as f:
		for line in f:
			links.append(line.strip());

	con=Connection();
	crawlStart=time.time();
	addcount=crawl(con, links, errlist);
	crawlEnd=time.time();

	with open('seeds', 'w') as f:
		for link in links:
			f.write(link+'\n');

	if len(errlist) > 0:
		with open('error.log', 'a') as f:
			for e in errlist:
				f.write('[{0}] {1}\n'.format(*e));

	print "crawled {0} pages in {1} seconds".format(addcount, crawlEnd-crawlStart);
	print "the database now contains {0} sites".format(con.crawldb.pages.find().count());
	con.disconnect();
