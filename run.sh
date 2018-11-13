#!/bin/bash
hdfs dfs -rm -r -f /user/rayandrew/twitter_output
hdfs dfs -rm -r -f /user/rayandrew/temp
rm Triangle*.class
hadoop com.sun.tools.javac.Main TriangleCount.java
jar cf triangle.jar Triangle*.class
hadoop jar triangle.jar TriangleCount /user/rayandrew/twitter_input /user/rayandrew/twitter_output
# hadoop jar triangle.jar TriangleCount /user/rayandrew/test /user/rayandrew/twitter_output