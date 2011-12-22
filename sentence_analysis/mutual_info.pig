-- pig -x local -f mutual_info.pig -p TOP_N=2 -p INPUT=/home/hadoop/part-r-00000
-- pig -f mutual_info.pig -p TOP_N=50000 -p INPUT=sentences.gz

set default_parallel 800;

-- load sentences
data = load '$INPUT' as (url:chararray, dts:long, pidx:int, sidx:int, sentence:chararray);
sentences = foreach data generate sentence;

-- calculate unigram/bigram frequency/counts
register cc.jar
define Unigrams cc.udfs.Unigrams();
define Bigrams cc.udfs.Bigrams();
unigrams = foreach sentences generate flatten(Unigrams(sentence));
bigrams  = foreach sentences generate flatten(Bigrams(sentence));
u_grped = group unigrams by $0;
unigram_f = foreach u_grped generate group as t1, SIZE(unigrams) as freq;
u_grped_all = group unigram_f all;
unigram_c = foreach u_grped_all generate SUM(unigram_f.freq) as count;
b_grped = group bigrams by ($0,$1);
bigram_f = foreach b_grped generate flatten(group), SIZE(bigrams) as freq;
bigram_f = foreach bigram_f generate $0 as t1, $1 as t2, freq;
b_grped_all = group bigram_f all;
bigram_c = foreach b_grped_all generate SUM(bigram_f.freq) as count;

-- store top bigrams / unigrams (for reference)
topn_u = foreach u_grped_all { i = TOP($TOP_N,1,unigram_f); generate flatten(i); }
store topn_u into '/home/hadoop/topN_unigrams.gz';
topn_b = foreach b_grped_all { i = TOP($TOP_N,2,bigram_f); generate flatten(i); }
store topn_b into '/home/hadoop/topN_bigrams.gz';

-- filter "near" hapax legomenon
bigram_f = filter bigram_f by freq > 100;

-- calculate bigram log likelihood; vanilla + bigram_weighted
-- mutual_info = log2( p(a,b) / ( p(a) * p(b) ) )
bigram_joined_1 = join bigram_f by t1, unigram_f by t1;
bigram_joined_2 = join bigram_joined_1 by t2, unigram_f by t1;
mutual_info = foreach bigram_joined_2 {
 t1 = bigram_joined_1::bigram_f::t1;
 t2 = bigram_joined_1::bigram_f::t2;
 t1_t2_f = bigram_joined_1::bigram_f::freq;
 t1_f = bigram_joined_1::unigram_f::freq;
 t2_f = unigram_f::freq;

 pxy = (double)t1_t2_f / bigram_c.count;
 px = (double)t1_f / unigram_c.count;
 py = (double)t2_f / unigram_c.count;
 mi = pxy * LOG(pxy / (px * py));

 generate t1 as t1, t2 as t2, t1_t2_f as t1_t2_f , 
  mi as mi, 
  LOG(pxy) * mi as log_weighted_mi;

}

-- topN by mutual_info and log_weighted_mutual_info
grped = group mutual_info all;
top_mutual_info = foreach grped { i = TOP($TOP_N,3,mutual_info); generate flatten(i); }
store top_mutual_info into '/home/hadoop/topN_mutual_info.gz';
top_log_weighted_mutual_info = foreach grped { i = TOP($TOP_N,4,mutual_info); generate flatten(i); }
store top_log_weighted_mutual_info into '/home/hadoop/topN_log_weighted_mutual_info.gz';
