CP="dist/hadoop-config.jar:/usr/local/hadoop/hadoop-1.0.3/hadoop-core-1.0.3.jar"
CP=${CP}:/usr/local/hadoop/hadoop-1.0.3/conf
for f in /usr/local/hadoop/hadoop-1.0.3/lib/*.jar 
do
CP=${CP}:${f}
done

java -classpath ${CP} bdpuh.hadoopconfiguration.ExplicitConfiguration2

