set -ex
hadoop fs -mkdir arc_file_manifest/$1
hadoop fs -copyFromLocal arc_files.$1 arc_file_manifest/$1

MAP_SLOTS=500
MAP_TASKS=$(($MAP_SLOTS*4))
SIZE=$(stat -c%s "arc_files.$1")
SPLIT=$(($SIZE/$MAP_TASKS))
hadoop jar cc.jar cc.SimpleDistCp \
 -D cc.hdfs_path=common_crawl_data/$1/ \
 -D mapred.max.split.size=$SPLIT \
 arc_file_manifest/$1 stdout/$1

hadoop jar cc.jar cc.ExtractVisibleTextFromArc \
 -libjars nutch-1.2.jar,boilerpipe-1.2.0.jar,nekohtml-1.9.13.jar,xerces-2.9.1.jar \
 -D mapred.output.compress=true -D mapred.output.compression.codec=org.apache.hadoop.io.compress.GzipCodec \
 common_crawl_data/$1/ visible_text/$1

