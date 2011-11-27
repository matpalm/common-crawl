set -ex

hadoop fs -mkdir arc_file_manifest/$1
hadoop fs -copyFromLocal manifest.$1 arc_file_manifest/$1

MAP_SLOTS=500
MAP_TASKS=$(($MAP_SLOTS*4))
SIZE=$(stat -c%s "manifest.$1")
SPLIT=$(($SIZE/$MAP_TASKS))
hadoop jar cc.jar cc.SimpleDistCp \
 -D mapred.max.split.size=$SPLIT \
 arc_file_manifest/$1 arc_files



