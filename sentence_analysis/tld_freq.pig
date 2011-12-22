-- pig -x local -f tld_freq.pig -p INPUT=/home/hadoop/part-r-00000 -p OUTPUT=/home/hadoop/foo
-- pig -f tld_freq.pig -p INPUT=sentences.gz -p OUTPUT=tld_freq_freq.gz
set default_parallel 800;
register cc.jar
define TopLevelDomain cc.udfs.TopLevelDomain();
data = load '$INPUT' as (url:chararray, dts:long, pidx:int, sidx:int, sentence:chararray);
tlds = foreach data generate TopLevelDomain(url) as tld;
grped = group tlds by tld;
tld_freq = foreach grped generate group as tld, SIZE(tlds) as freq;

--high_freq = filter tld_freq by freq > 50000;
--store high_freq into 'high_freq_tld.gz';

grped2 = group tld_freq by freq;
tld_freq_freq = foreach grped2 generate group as freq, SIZE(tld_freq) as freq_freq;
store tld_freq_freq into '$OUTPUT';
