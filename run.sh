#!/bin/bash

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
# hadoop jar ttp.jar TriangleTypePartition /user/rayandrew/links_anon_input /user/rayandrew/links_anon_output 64

# Uncomment to use twitter-rv file
# Use 64 partitions
# hdfs dfs -rm -r -f /user/rayandrew/twitter_output
# hadoop jar ttp.jar TriangleTypePartition /data/twitter /user/rayandrew/twitter_output 64

# Uncomment to use testcase 1 file
# Use 3 partitions
# hdfs dfs -rm -r -f /user/rayandrew/testcase1_output
# hadoop jar ttp.jar TriangleTypePartition /data/testcase1 /user/rayandrew/testcase1_output 3

# Uncomment to use testcase 2 file
# Use 5 partitions
# hdfs dfs -rm -r -f /user/rayandrew/testcase2_output
# hadoop jar ttp.jar TriangleTypePartition /data/testcase2 /user/rayandrew/testcase2_output 5
