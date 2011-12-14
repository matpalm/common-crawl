hadoop jar cc.jar cc.RunAll \
 -libjars nutch-1.2.jar,tika-app-1.0.jar,stanford-parser.jar,boilerpipe-1.2.0.jar,nekohtml-1.9.13.jar,xerces-2.9.1.jar \
 -D mapred.max.map.failures.percent=100 \
 arc_files.$1/*/*/*/*/* sentences_$1
