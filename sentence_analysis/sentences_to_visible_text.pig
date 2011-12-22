-- recombined sentences (order by paragraph and sentence) to make a single entry per url/dts
-- pig -f sentences_to_visible_text.pig -p INPUT=foo -p OUTPUT=bar
set default_parallel 800;
register cc.jar
define StrJoin cc.udfs.StrJoin;
data = load '$INPUT' as (url:chararray, dts:long, pidx:int, sidx:int, sentence:chararray);
by_url_dts = group data by (url,dts);
combined_text = foreach by_url_dts {
 sorted = order data by pidx, sidx;
 generate flatten(group), StrJoin(data.sentence) as sentences;
}
store combined_text into '$OUTPUT';
