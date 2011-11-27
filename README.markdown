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

download the data using jets3t. was using common crawl input format (which did the download) but had lots of problems.

    see simple_dist_cp.sh

### pass 1) filter text/html

single map only pass that uses the nutch arc input format

ignore everything but mime_type 'text/html'

want to just have this so can do experiments in either link graph or visible text

    see filter_text_html.sh

### pass 2 ) visible text extraction

pass html through boilerpipe to get visible text

pass visible text through tika to identify language

ignore everything but language 'en'
 
emit into sequence file with reduce step to compact into fewer files

    see extract_visible_english_text.sh

************** TODO 


filter on mime_type = text/html records 
emits records; key= url/dts value= html
reduces dataset to 10TB gzip compressed, 2.1e9 records

    see fetch_text_html.sh

### pass 2) visible text

pass data through <a href="http://code.google.com/p/boilerpipe/">boilerpipe</a> to extract visible text for each webpage. 

ignore pages that are filtered by <a href="http://boilerpipe.googlecode.com/svn/trunk/boilerpipe-core/javadoc/1.0/de/l3s/boilerpipe/extractors/KeepEverythingWithMinKWordsExtractor.html">KeepEverythingWithMinKWordsExtractor</a> (K=10)

ignore pages that have have no spaces

record key is url \t dts
record value is visible text. multiple line, each line appears to be from a seperate page element.

reduces data to ?? gzip compressed

    see extract_visible_text.sh

### pass 3) compact

reads 280,000 files from pass2) and reduces to 3,000 sequence files

    hadoop jar cc.jar cc.Compact -D mapred.reduce.tasks=123 input_dir output_dir

### pass 4) filter english documents

### pass 5) tokenise with <a href="http://nlp.stanford.edu/software/lex-parser.shtml">the stanford parser</a>

### pass 6) collocation extraction via mutual information


