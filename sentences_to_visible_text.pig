-- recombined sentences (order by paragraph and sentence) to make a single entry per url
-- pig -f sentences_to_visible_text.pig -p INPUT=foo -p OUTPUT=bar
register piggybank.jar;
define SequenceFileLoader org.apache.pig.piggybank.storage.SequenceFileLoader();
register cc.jar;
define StrJoin cc.udfs.StrJoin;

raw = load '$INPUT' using SequenceFileLoader as (url_pidx_sidx:chararray, sentence:chararray);
data = foreach raw generate flatten(STRSPLIT(url_pidx_sidx)), sentence;
data = foreach data generate (chararray)$0 as url, (int)$1 as pidx, (int)$2 as sidx, (chararray)$3 as sentence;

by_url = group data by url;
combined_text = foreach by_url {
 sorted = order data by pidx, sidx;
 generate group as url, StrJoin(sorted.sentence) as sentences;
}
store combined_text into '$OUTPUT';
