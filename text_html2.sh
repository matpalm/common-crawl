# eg sh text_html2.sh 2010/04 2010_04
hadoop jar cc.jar cc.FilterTextHtml2 \
 -libjars nutch-1.2.jar,tika-app-1.0.jar,commoncrawl-0.1.jar,jets3t-0.8.1.jar \
 -D mapreduce.job.counters.limit=1000 \
 $1 text_html.$2
