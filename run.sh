#!/bin/bash

# Uncomment this if needed
# hdfs dfs -rm -r -f /user/rayandrew/test_output
# hdfs dfs -rm -r -f /user/rayandrew/links_anon_output
# hdfs dfs -rm -r -f /user/rayandrew/twitter_rv_output
hdfs dfs -rm -r -f /user/rayandrew/temp

# rm Triangle*.class
# hadoop com.sun.tools.javac.Main TriangleCount.java
# jar cf triangle.jar Triangle*.class

# rm Graph*.class LongPair*.class
# hadoop com.sun.tools.javac.Main GraphPartition.java Graph.java LongPair.java
# jar cf graph.jar Graph*.class LongPair*.class

rm TriangleTypePartition*.class LongPair*.class
hadoop com.sun.tools.javac.Main TriangleTypePartition.java LongPair.java
jar cf ttp.jar TriangleTypePartition*.class LongPair*.class

# Uncomment to use links-anon file
# hadoop jar ttp.jar TriangleTypePartition /user/rayandrew/links_anon_input /user/rayandrew/links_anon_output

# Uncomment to use twitter-rv file
# Use 64 partitions
hadoop jar ttp.jar TriangleTypePartition /data/twitter /user/rayandrew/twitter_output 64

# Uncomment to use test file
# hadoop jar ttp.jar TriangleTypePartition /user/rayandrew/test_input /user/rayandrew/test_output
