-- pig	-f calculate_frequencies.pig -p	INPUT=/foo/bar
set default_parallel 110;
register piggybank.jar;
define SequenceFileLoader org.apache.pig.piggybank.storage.SequenceFileLoader();
raw = load '$INPUT' using SequenceFileLoader as (url_pidx_sidx:chararray, sentence:chararray);
data = foreach raw generate flatten(STRSPLIT(url_pidx_sidx)), sentence;
data = foreach data generate (chararray)$0 as url, (int)$1 as pidx, (int)$2 as sidx, (chararray)$3 as sentence;

urls = foreach data generate url;
urls_grouped = group urls by url;
url_freq = foreach urls_grouped {
 generate SIZE(urls) as freq;
}
freq_grouped = group url_freq by freq;
freq_freq = foreach freq_grouped {
 generate group as freq, SIZE(url_freq) as freq_freq;
}
-- number_of_sentences_for_url, freq_freq
store freq_freq into 'url_freq_freqs.gz';

sentence_lengths = foreach data generate SIZE(STRSPLIT(sentence)) as len;
sentence_lengths_grouped = group sentence_lengths by len;
sentence_length_freq = foreach sentence_lengths_grouped {
 generate group as length, SIZE(sentence_lengths) as freq;
}
-- length, freq
store sentence_length_freq into 'sentence_length_freq.gz';
