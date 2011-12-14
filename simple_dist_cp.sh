if [ $# -ne 1 ]; then
 echo "usage: $0 <manifest_file_num>"
 exit 1
fi

set -ex

if hadoop fs -test -d arc_file_manifest; then
 hadoop fs -rmr arc_file_manifest 
fi

if hadoop fs -test -d SimpleDistCp.out; then
 hadoop fs -rmr SimpleDistCp.out
fi
 
hadoop fs -mkdir arc_file_manifest/$1
hadoop fs -copyFromLocal manifest.$1 arc_file_manifest/$1

hadoop jar cc.jar cc.SimpleDistCp \
 -D mapred.map.tasks=1000 \
 -D mapred.map.multithreadedrunner.threads=5 \
 -D mapred.max.map.failures.percent=100 \
 arc_file_manifest/$1 arc_files.$1
