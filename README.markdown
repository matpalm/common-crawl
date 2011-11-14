# playing with the common crawl

*serious work in progess*

<a href="http://www.commoncrawl.org">common crawl</a> is a freely available 25+TB webcrawl.

## dependencies

* <a href="http://jets3t.s3.amazonaws.com/index.html">jets3t</a> for getting data (requester pays support)
* <a href="http://nutch.apache.org/">nutch</a> for the <a href="http://nutch.apache.org/apidocs-1.2/org/apache/nutch/tools/arc/ArcInputFormat.html">ArcInputFormat</a>
* <a href="http://code.google.com/p/boilerpipe/">boilerpipe</a> for extracting visible text from html (the 
<a href="http://boilerpipe.googlecode.com/svn/trunk/boilerpipe-core/javadoc/1.0/de/l3s/boilerpipe/extractors/KeepEverythingWithMinKWordsExtractor.html">
KeepEverythingWithMinKWordsExtractor</a> has been working well for me...
* <a href="http://nlp.stanford.edu/software/lex-parser.shtml">the stanford parser</a> for general NLP witchcraft.

## method

    # get a sample of 100gb; download to common_crawl_data on hdfs
    zcat arc_files.gz | head -n1000 > sample_arc_file_paths # each arc file is 100mb
    hadoop fs -mkdir manifest
    hadoop fs -copyFromLocal sample_arc_file_paths manifest
    hadoop jar cc.jar cc.SimpleDistCp \
     -D mapred.max.split.size=250 \
     -D cc.hdfs_path=common_crawl_data/ \
     manifest stdout

    # extract visible text (playing with dependencies still)
    hadoop jar cc.jar cc.ExtractVisibleTextFromArc \
     -libjars nutch-1.2.jar,boilerpipe-1.2.0.jar,nekohtml-1.9.13.jar,xerces-2.9.1.jar \
     common_crawl_data visible_text

 
 

