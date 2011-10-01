#!/usr/bin/python

from multiprocessing import *
from urlparse import urlparse
from urllib import urlopen
from pymongo import *
from pymongo.code import Code
import time
import sys
import re

if __name__ == '__main__':
	con=Connection()
	db=con.crawldb
	pages=db.pages
	if pages.count() == 0:
		print 'there are no pages to read!'
		sys.exit(1)

	m=Code("function() {"
			+"	var wfind=/[a-z]+/gi;"
			+" var wordlist=this._page.match(wfind);"
			+"	if (wordlist) {"
			+"		words={};"
			+"		wordlist.forEach(function (word) {"
			+"			word=word.toLowerCase();"
			+"			if (word in words) { words[word]++; }"
			+"			else { words[word]=1; }"
			+"		});"
			+"		emit(this._url, words);"
			+"	}"
			+"	else { emit(this._url, {}); }"
			"}");
	r=Code("function(key, values) { return values[0]; }")
	mr=pages.map_reduce(m, r, {'replace':'__words'}, query={'_words':False})
	for r in mr.find(): 
		if len(r['value']) > 0:
			pages.update({'_url': r['_id']}, {'$set': {'_words': r['value']}})
	print 'updated {0} pages'.format(mr.count())
	print 'done updating pages; updating words'

#	Map Reduce code would probably run faster if my laptop were not running the database...

	m=Code("function () { for (w in this._words) { emit(w, this._words[w]); } }")
	r=Code("function (key, values) {"
			+"	var total=0;"
			+"	for (var i=0; i < values.length; ++i) {"
			+"		total += values[i];"
			+"	}"
			+"	return total;"
			+"}")

	db.pages.map_reduce(m, r, {'reduce':'words'}, 
			query={'_words': {'$ne': False}, '_wordsAdded': False})

	db.pages.update({'_words': {'$ne': False}, '_wordsAdded': False},
			{'$set': {'_wordsAdded': True}}, multi=True)
