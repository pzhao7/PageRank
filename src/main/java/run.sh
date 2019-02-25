hdfs dfs -rm -r /transition
hdfs dfs -mkdir /transition
hdfs dfs -put transition.txt /transition
hdfs dfs -rm -r /output*
hdfs dfs -rm -r /pagerank*
hdfs dfs -mkdir /pagerank0
hdfs dfs -put pr.txt /pagerank0
hadoop com.sun.tools.javac.Main *.java
jar cf pr.jar *.class
hadoop jar pr.jar Driver /transition /pagerank /output 5 0.5
hdfs dfs -text /pagerank1/* | head -n 10
hdfs dfs -text /pagerank5/* | head -n 10