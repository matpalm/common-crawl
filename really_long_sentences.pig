-- pig	-f calculate_frequencies.pig -p	INPUT=/foo/bar
set default_parallel 110;
register piggybank.jar;
define SequenceFileLoader org.apache.pig.piggybank.storage.SequenceFileLoader();
raw = load 'sentences' using SequenceFileLoader as (url_pidx_sidx:chararray, sentence:chararray);
raw = sample raw 0.01;
data = foreach raw generate flatten(STRSPLIT(url_pidx_sidx)), sentence;
data = foreach data generate (chararray)$0 as url, (int)$1 as pidx, (int)$2 as sidx, (chararray)$3 as sentence;
sentence_lengths = foreach data generate url, pidx, sidx, SIZE(STRSPLIT(sentence)) as len, sentence;
long_sentences = filter sentence_lengths by len > 10000;
long_sentences = limit long_sentences 10000;
store long_sentences into 'long_sentences.gz';

