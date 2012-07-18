-- compact runs into a single set as well as changing from sequence file to calculate frequency of frequencies of crawl
-- pig -f url_crawl_freq_freq.pig
-- eg 
-- 1,20 <- 20 urls crawled once
-- 2,5  <- 5 urls crawled twice

 register piggybank.jar;
 define SequenceFileLoader org.apache.pig.piggybank.storage.SequenceFileLoader();
 raw1 = load 'sentences.2009_09' using SequenceFileLoader as (url_dts_pidx_sidx:chararray, sentence:chararray);
 raw2 = load 'sentences.2009_11' using SequenceFileLoader as (url_dts_pidx_sidx:chararray, sentence:chararray);
 raw3 = load 'sentences.2010_01' using SequenceFileLoader as (url_dts_pidx_sidx:chararray, sentence:chararray);
 raw4 = load 'sentences.2010_02' using SequenceFileLoader as (url_dts_pidx_sidx:chararray, sentence:chararray);
 raw5 = load 'sentences.2010_04' using SequenceFileLoader as (url_dts_pidx_sidx:chararray, sentence:chararray);
 raw6 = load 'sentences.2010_08' using SequenceFileLoader as (url_dts_pidx_sidx:chararray, sentence:chararray);
 raw7 = load 'sentences.2010_09' using SequenceFileLoader as (url_dts_pidx_sidx:chararray, sentence:chararray);
 raw = union raw1, raw2, raw3, raw4, raw5, raw6, raw7;
 data = foreach raw generate flatten(STRSPLIT(url_dts_pidx_sidx)), sentence;
 data = foreach data generate (chararray)$0 as url, (long)$1 as dts, (int)$2 as pidx, (int)$3 as sidx, (chararray)$4 as sentence;
 store data into 'sentences.gz'

