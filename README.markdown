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

outputs (as sequence file)
key: url
value: html

see text_html.sh
  
### pass 2 ) visible text extraction

pass html through boilerpipe to get visible text

pass visible text through tika to identify language

ignore everything but language 'en'
 
outputs (as sequence file)
key: url
value: visible text, each line represents text from a block element of html; i'll call this a paragraph from now on

see visible_en_text.sh

### pass 3 ) tokenisation

pass visible text, a paragraph at a time, through stanford parser and extract sentences / tokens

we only emit each sentence _once_ per page since the vast majority represent noise (header/footer/list structures etc)

outputs (as sequence file)
key: url \t paragraph_idx \t sentence_in_paragraph_idx
value: one sentence, tokens space seperated

see sentences.sh


from here we have lots of options

- remit with just top level domain / sentence and dedup
- deduping at page level



