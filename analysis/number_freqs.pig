-- pig -f number_freqs.pig -p IN=sentences_0.00001.gz -p OUT=number_freqs_0.00001.top200.gz -p TOP_N=100
-- pig -f number_freqs.pig -p IN=sentences_0.001.gz -p OUT=number_freqs_0.001.top10k.gz -p TOP_N=10000
set default_parallel 200;
data = load '$IN' as (url, dts, pidx, sidx, sentence:chararray);
sentences = foreach data generate sentence;
define extract_numbers `python extract_numbers.py` ship('/home/hadoop/extract_numbers.py');
numbers = stream sentences through extract_numbers as (number:chararray);
grped = group numbers by number;
freqs = foreach grped generate group as number, SIZE(numbers) as freq;
freqs = filter freqs by freq>1;
g = group freqs all;
h = foreach g { i = TOP($TOP_N,1,freqs); generate flatten(i); }
h = order h by freq desc;
store h into '$OUT';
