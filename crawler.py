#!/usr/bin/python

from multiprocessing import *;
from urllib import urlopen;
from pymongo import *;
import pickle;
import time;
import sys;
import re;

domfinder=re.compile("^http://[^/]*", flags=re.I);
linkfinder=re.compile("<a href=.?(http://[^ >'\"]+)[^>]*>", flags=re.I);
wordfinder=re.compile("([a-z]+)('[a-z])?", flags=re.I);
tagkiller=re.compile("(<style.*?</style>)|(<script.*?</script>)|(<noscript.*?</noscript>)|(<.*?>)", flags=re.S|re.X);

if len(sys.argv) < 4 or len(sys.argv) > 4:
	print 'usage: {0} pages children timelimit'.format(sys.argv[0]);
	sys.exit(0);

MinPages=int(sys.argv[1]);
MaxChildren=int(sys.argv[2]);
TimeLimit=int(sys.argv[3]);

class site(object):
	
	def __init__(self, url):
		self._url=url;
		self._domain=domfinder.search(url).group(0);
		self._page=urlopen(url).read();
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

def visitpage(l, pagehash, links, errors):

	if l in pagehash:
		s=pagehash[l];
		s.indegree+=1;
		pagehash[l]=s;

	else:
		try:
			s=site(l);
			pagelinks=linkfinder.findall(s.page);
			s.indegree=1;
			s.outdegree=len(pagelinks);
			pagehash[l]=s;

			for link in pagelinks:
				links.put(link);

		except Exception as e:
			errors.append((time.ctime(), e)); 

def crawl(pagehash, links, errors, start=time.time()):
	i=0;
	while (len(pagehash) < MinPages) and ((time.time() - start) < TimeLimit):
		if len(active_children()) - 1 < MaxChildren:
			try:
				link=links.get(False);
				Process(name="visitor-"+str(i), target=visitpage, 
							args=(link, pagehash, links, errors)).start();
				i+=1;
			except Exception as e:
				errors.append((time.ctime(), e));

	print "done crawling";

	for child in active_children():
		if re.match("visitor", child.name):
			child.join();

	print 'done cleanup';

if __name__ == '__main__':

	seeds=set(['http://www.cnn.com', 'http://www.huffingtonpost.com', 'http://www.reddit.com']);

	manager=Manager();
	pagehash=manager.dict();
	links=manager.Queue();
	errors=manager.list();

	for seed in seeds:
		try:
			s=site(seed);
			pagelinks=linkfinder.findall(s.page);
			for link in pagelinks:
				links.put(link);
			s.outdegree=len(pagelinks);
			pagehash[seed]=s;

		except Exception as e:
			errors.append((time.ctime(), e));

	crawlStart=time.time();
	crawl(pagehash, links, errors);
	crawlEnd=time.time();

	records=[];
	for url in dict(pagehash):
		print pagehash[url];
		records.append(pagehash[url].__dict__);
	con=Connection();
	con['crawldb']['pages'].insert(records);
	con.disconnect();

	if len(errors) > 0:
		with open('error.log', 'w') as f:
			for e in errors:
				f.write('[{0}] {1}\n'.format(*e));

	print "crawled {0} pages in {1} seconds".format(len(pagehash), crawlEnd-crawlStart);
