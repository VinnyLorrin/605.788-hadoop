CP="dist/mapreduce-basics.jar:/usr/local/hadoop/hadoop-1.0.3/hadoop-core-1.0.3.jar"
CP=${CP}:/usr/local/hadoop/hadoop-1.0.3/conf
for f in /usr/local/hadoop/hadoop-1.0.3/lib/*.jar 
do
CP=${CP}:${f}
done

hadoop fs -rmr /out

#java -classpath ${CP} bdpuh.mapreducebasics.WordCount /in /out
hadoop jar dist/mapreduce-basics.jar  bdpuh.mapreducebasics.WordCount /in /out
