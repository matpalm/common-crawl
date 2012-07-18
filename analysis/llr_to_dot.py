#!/usr/bin/env python
import sys
from collections import *
from math import *

bigram_weights = defaultdict(list)

def bigrams(ngram, llr):
    bigrams = []
    for i in range(len(ngram)-1):
        t = tuple([ngram[i], ngram[i+1]])
        bigram_weights[t].append(llr)

min_weight = max_weight = None
for line in sys.stdin:
    (ngram, size, llr) = line.split("\t")
    ngram = ngram.split()
    llr = float(llr)
    if min_weight == None or llr < min_weight: min_weight = llr
    if max_weight == None or llr > max_weight: max_weight = llr
#    print ngram, llr, min_weight, max_weight
    bigrams(ngram, llr)
#    print bigram_weights

delta = max_weight - min_weight

print "digraph {"
for bigram, weights in bigram_weights.iteritems():
#    print "bigram", bigram, "weights", weights
    t1,t2 = bigram
#    print "before", weights
    normalised_weights = [ (w-min_weight)/delta for w in weights]
    rescaled_weights = [ 1 + (5*w) for w in normalised_weights ]
#    print "after", weights
    weight = float(sum(rescaled_weights)) / len(rescaled_weights)
    # print "\"" + t1 + "\" -- \"" + t2 + "\" [penwidth=\"" + str(weight) + "\"]"
    print "\"%s\" -> \"%s\" [penwidth=\"%s\"];" % (t1,t2,weight)
print "}"
