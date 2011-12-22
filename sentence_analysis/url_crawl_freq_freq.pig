-- calculate frequency of frequencies of crawl
-- pig -f url_crawl_freq_freq.pig
-- eg 
-- 1,20 <- 20 urls crawled once
-- 2,5  <- 5 urls crawled twice
 register piggybank.jar;
 data = load 'sentences.gz' as (url:chararray, dts:long, pidx:int, sidx:int, sentence:chararray);
 first_sentences = filter data by pidx==0 and sidx==0;
 just_url = foreach first_sentences generate url;
 grped = group just_url by url;
 url_freqs = foreach grped generate SIZE(just_url) as freq;
 grped2	= group url_freqs by freq;
 url_freq_freqs = foreach grped2 generate group	as freq, SIZE(url_freqs) as freq_freq;
 store url_freq_freqs into 'url_crawl_freq_freqs.gz';
