import re
import pymongo
from tqdm import tqdm
import hashlib

db = pymongo.MongoClient().weixin.text_articles
md5 = lambda s: hashlib.md5(s).hexdigest()
def texts():
	texts_set = set()
	for a in tqdm(db.find(no_cursor_timeout=True).limit(3000000)):
		if md5(a['text'].encode('utf-8')) in texts_set:
			continue
		else:
			texts_set.add(md5(a['text'].encode('utf-8')))
			for t in re.split(u'[^\u4e00-\u9fa50-9a-zA-Z]+', a['text']):
				if t:
					yield t
	print u'最终计算了%s篇文章'%len(texts_set)

from collections import defaultdict
import numpy as np

n = 4
min_count = 128
min_proba = {2:5, 3:25, 4:125}
ngrams = defaultdict(int)

for t in texts():
	for i in range(len(t)):
		for j in range(1, n+1):
			if i+j <= len(t):
				ngrams[t[i:i+j]] += 1

ngrams = {i:j for i,j in ngrams.iteritems() if j >= min_count}
total = 1.*sum([j for i,j in ngrams.iteritems() if len(i) == 1])

def is_keep(s, min_proba):
	if len(s) >= 2 and min([total*ngrams[s]/(ngrams[s[:i+1]]*ngrams[s[i+1:]]) for i in range(len(s)-1)]) > min_proba[len(s)]:
		return True
	else:
		return False

ngrams_ = set(i for i,j in ngrams.iteritems() if is_keep(i, min_proba))

def cut(s):
	r = np.array([0]*(len(s)-1))
	for i in range(len(s)-1):
		for j in range(2, n+1):
			if s[i:i+j] in ngrams_:
				r[i:i+j-1] += 1
	w = [s[0]]
	for i in range(1, len(s)):
		if r[i-1] > 0:
			w[-1] += s[i]
		else:
			w.append(s[i])
	return w

words = defaultdict(int)
for t in texts():
	for i in cut(t):
		words[i] += 1

words = {i:j for i,j in words.iteritems() if j >= min_count}

def is_real(s):
	if len(s) >= 3:
		for i in range(3, n+1):
			for j in range(len(s)-i+1):
				if s[j:j+i] not in ngrams_:
					return False
		return True
	else:
		return True

w = {i:j for i,j in words.iteritems() if is_real(i)}

import pandas as pd
w = pd.Series(w)
w = w.sort_values(ascending=False)
w.to_csv('words.txt', encoding='utf-8', header=None)