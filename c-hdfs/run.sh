HADOOP_HOME=/usr/local/hadoop/hadoop-1.0.3
JAVA_HOME=/usr/lib/jvm/java
export LD_LIBRARY_PATH=${HADOOP_HOME}/c++/Linux-amd64-64/lib:${JAVA_HOME}/jre/lib/amd64/server
export CLASSPATH="/usr/local/hadoop/hadoop-1.0.3/hadoop-core-1.0.3.jar"
CLASSPATH=${CLASSPATH}:/usr/local/hadoop/hadoop-1.0.3/conf
for f in /usr/local/hadoop/hadoop-1.0.3/lib/*.jar
do
CLASSPATH=${CLASSPATH}:${f}
done
./create-file
