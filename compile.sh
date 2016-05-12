rm -r class/*
javac -Xlint -classpath $HADOOP_HOME/share/hadoop/common/hadoop-common-2.7.2.jar:$HBASE_HOME/lib/hbase-common-1.2.1.jar:$HBASE_HOME/lib/hbase-client-1.2.1.jar -d class code/*
jar -cvf HBaseExample.jar -C class/ .
