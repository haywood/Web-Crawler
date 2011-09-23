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

class site(object):
	
	def __init__(self, url):
		self._url=url;
		self._domain=urlparse(url).netloc;
		self._page=unescape(urlopen(url).read());
		self._ideg=0;
		self._odeg=0;
		self._words=False;

	@property
	def url(self):
		return self._url;

	@property
	def domain(self):
		return self._domain;

	@property
	def page(self):
		return self._page;

	def indegree():
		def fget(self):
			return self._ideg;

		def fset(self, ideg):
			self._ideg=ideg;
		
		return locals();

	indegree=property(**indegree());

	@property
	def words(self):
		if self._words==False:
			words=tagkiller.sub(' ', self.page);
			words=wordfinder.finditer(words);
			self._words=dict();

			for word in words:
				w=word.group(1).lower();
				if w in self._words:
					self._words[w]+=1;
				else: self._words[w]=1;
		return self._words;


	def outdegree():
		def fget(self):
			return self._odeg;

		def fset(self, odeg):
			self._odeg=odeg;

		return locals();

	outdegree=property(**outdegree());

	def __str__(self):
		s="url: "+self.url;
		s+="\ndomain: "+self.domain;
		s+="\nindegree: "+str(self.indegree);
		s+="\noutdegree: "+str(self.outdegree);
		return s;

def visitpage(con, l, links, newpage, errlist):

	pages=con.crawldb.pages;
	try:
		if pages.find_one({'_url':l}):
			pages.update({'_url':l}, {'$inc':{'_ideg':1}});
	except errors.AutoReconnect as e:
		errlist.append((time.ctime(), e));

	else:
		try:
			s=site(l).__dict__;
			pagelinks=linkfinder.findall(s['_page']);
			s['_outdegree']=len(pagelinks);
			s['_indegree']=1;
			pages.insert(s);

			for link in pagelinks:
				if not pages.find_one({'_url': link}):
					links.put(link);

			newpage.put(l);

		except Exception as e:
			errlist.append((time.ctime()+' '+l, e)); 

def crawl(con, links, newpage, errlist, start=time.time()):
	i=0;
	while (newpage.qsize() < MinPages) and ((time.time() - start) < TimeLimit):
		if len(active_children()) - 1 < MaxChildren:
			try:
				link=links.get(False);
				Process(name="visitor-"+str(i), target=visitpage, 
							args=(con, link, links, newpage, errlist)).start();
				i+=1;
			except Exception as e:
				errlist.append((time.ctime(), e));

	print 'done crawling';

	for child in active_children():
		if re.match("visitor", child.name):
			child.join();

	print 'done cleanup';

if __name__ == '__main__':

	manager=Manager();
	links=manager.Queue();
	errlist=manager.list();
	newpage=manager.Queue();

	with open('seeds') as f:
		for line in f:
			links.put(line.strip());

	con=Connection();
	crawlStart=time.time();
	crawl(con, links, newpage, errlist);
	crawlEnd=time.time();

	with open('seeds', 'w') as f:
		while links.qsize() > 0:
			f.write(links.get()+'\n');

	if len(errlist) > 0:
		with open('error.log', 'a') as f:
			for e in errlist:
				f.write('[{0}] {1}\n'.format(*e));

	print "crawled {0} pages in {1} seconds".format(newpage.qsize(), crawlEnd-crawlStart);
	print "the database now contains {0} sites".format(con.crawldb.pages.find().count());
	con.disconnect();
