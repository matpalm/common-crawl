# playing with the common crawl

*serious work in progess*

<a href="http://www.commoncrawl.org">common crawl</a> is a freely available 25+TB webcrawl.

## dependencies

* <a href="http://jets3t.s3.amazonaws.com/index.html">jets3t</a> for getting data (requester pays support)
* <a href="http://nutch.apache.org/">nutch</a> for the <a href="http://nutch.apache.org/apidocs-1.2/org/apache/nutch/tools/arc/ArcInputFormat.html">ArcInputFormat</a>
* <a href="http://code.google.com/p/boilerpipe/">boilerpipe</a> for extracting visible text from html (the 
<a href="http://boilerpipe.googlecode.com/svn/trunk/boilerpipe-core/javadoc/1.0/de/l3s/boilerpipe/extractors/KeepEverythingWithMinKWordsExtractor.html">
KeepEverythingWithMinKWordsExtractor</a> has been working well for me...
* <a href="http://tika.apache.org/">tika</a> for language detection.
* <a href="http://nlp.stanford.edu/software/lex-parser.shtml">the stanford parser</a> for general NLP witchcraft.

## method

### pass 0) download the data

download the data using jets3t from s3 unmodified to hdfs. was using common crawl input format (which did the download) but had lots of problems.

see simple_dist_cp.sh

### pass 1) filter text/html

map only pass using the nutch arc input format to ignore everything but mime_type 'text/html'

also converts from raw http response (ie ascii headers + encoded bytes) to just utf-8 encoded html

want to just have this so can do experiments in either link graph or visible text

outputs (as sequence file) key: url, value: html response (utf-8 encoded)

see text_html.sh
  
### pass 2 ) visible text extraction

map only pass html through boilerpipe to extract visible text

uses the boilerpipe KeepEverythingWithMinKWordsExtractor to ignore block elements that don't have at least 5 terms

outputs (as sequence file) key: url, value: visible text, each line denotes a seperate block element from html

### pass 3) filter english text only

map only pass visible text through tika to identify language and ignore everything but language 'en'
 
outputs (as sequence file) key: url value: visible text

see visible_en_text.sh

### pass 4 ) tokenisation

map/reduce pass visible text, a paragraph at a time, through the stanford parser and extract sentences / tokens

ignore a sentence that tokens to less than 3 terms.

only emit each sentence _once_ per page since the vast majority of these duplicates represent noise (headers / footers / list structures etc)

outputs (as sequence file) key: url \t paragraph_idx \t sentence_in_paragraph_idx value: one sentence, tokens space seperated

#reducers ~= 3gb to get under 5gb s3 limit (ie sans multipart upload)

see sentences.sh

### pass 2 -> pass 4

see run.sh for a ChainMapper version that does steps 2 -> 4 in a single map/reduce pass




