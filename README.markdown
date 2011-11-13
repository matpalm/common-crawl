# playing with the common crawl

<a href="http://www.commoncrawl.org">common crawl</a> is a freely available webcrawl.

## dependencies

* <a href="http://jets3t.s3.amazonaws.com/index.html">jets3t</a> for getting data
* <a href="http://nutch.apache.org/">nutch</a> for the <a href="http://nutch.apache.org/apidocs-1.2/org/apache/nutch/tools/arc/ArcInputFormat.html">ArcInputFormat</a>
* <a href="http://code.google.com/p/boilerpipe/">boilerpipe</a> for extracting visible text from html (the 
<a href="http://boilerpipe.googlecode.com/svn/trunk/boilerpipe-core/javadoc/1.0/de/l3s/boilerpipe/extractors/KeepEverythingWithMinKWordsExtractor.html">
KeepEverythingWithMinKWordsExtractor</a> has been working well for me...
* <a href="http://nlp.stanford.edu/software/lex-parser.shtml">the stanford parser</a> for general NLP witchcraft.

SERIOUS WORK IN PROGRESS

 

