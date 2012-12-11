HADOOP_HOME=/usr/local/hadoop/hadoop-1.0.3
JAVA_HOME=/usr/lib/jvm/java
gcc create_file.c -I${HADOOP_HOME}/src/c++/libhdfs -I${JAVA_HOME}/include -I $JAVA_HOME/include/linux -L${HADOOP_HOME}/c++/Linux-amd64-64/lib -L${JAVA_HOME}/jre/lib/amd64/server -lhdfs -ljvm -o create-file
