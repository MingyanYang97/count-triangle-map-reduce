import java.lang.*;
import java.io.*;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.*;

/**
 * MapReduce using modified NodeIterator++ algorithm to count triangles in a graph.
 * Input format: lines each containing 2 integers which represent edges.
 * Input graph will be converted to an undirected graph, and duplicate edges will be removed.
 * By Ray Andrew (13515073) and Jonathan Christopher (13515001).
 * 
 * - MapperOne: (a, b) -> a < b ? emit (a < b) : emit (b > a)
 * - ReducerOne: emit single edges and edge pairs
 *   - remove duplicates of b
 *   - emit single edges ((x, y), SINGLE_EDGE)
 *   - emit all edge pairs of a,x and a,y ((x, y), a) where x > a and y > a
 *   
 * - MapperTwo: no-op
 * - ReducerTwo: count triangles which contain each edge pair
 *   - match on single edges and edge pairs
 *   - if edge pairs are connected to a single edge, count the number of edge pairs and emit it
 * 
 * - MapperThree: no-op
 * - ReducerThree: sum all triangle counts
 */
public class TriangleCount extends Configured implements Tool {

  public static final LongWritable SINGLE_EDGE = new LongWritable(-1);
  public static final Text RESULT_KEY = new Text('triangleCount');

  public static class MapperOne extends Mapper<LongWritable, Text, LongWritable, LongWritable> {
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
      String[] str = value.toString().split("\\s+");
      if (str.length > 1) {
        long node1 = Long.parseLong(str[0]);
        long node2 = Long.parseLong(str[1]);

        // Ensure the left node index is less than the right node index
        if (node1 < node2) {
          context.write(new LongWritable(node1), new LongWritable(node2));
        } else {
          context.write(new LongWritable(node2), new LongWritable(node1));
        }
      }
    }
  }

  public static class ReducerOne extends Reducer<LongWritable, LongWritable, Text, Text> {
    public void reduce(LongWritable key, Iterable<LongWritable> values, Context context)
        throws IOException, InterruptedException {
      
      // Insert values to a set to remove duplicates
      Set<LongWritable> valuesSet = LinkedHashSet();
      for (LongWritable node : values) {
        valuesSet.add(node);
      }

      // Insert unique values to list to ease combination generation
      List<LongWritable> uniqueValues = ArrayList(valuesSet.size());
      for (LongWritable node : valuesSet) {
        uniqueValues.add(node);
      }

      // Emit single edge
      context.write(
        new Text(Long.toString(key) + ',' + Long.toString(node)),
        new Text(Long.toString(SINGLE_EDGE))
      );

      // Emit all edge pairs which are connected on the key node
      for (int i = 0; i < uniqueValues.size(); i++) {
        for (int j = i + 1; j < uniqueValues.size(); j++) {
          context.write(
            new Text(Long.toString(uniqueValues[i]) + ',' + Long.toString(uniqueValues[j])),
            new Text(Long.toString(key))
          );
        }
      }
    }
  }

  public static class MapperTextLongWritable extends Mapper<LongWritable, Text, Text, LongWritable> {
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
      String[] str = value.toString().split("\\s+");
      if (str.length > 1) {
        context.write(
          new Text(str[0]),
          new LongWritable(Long.parseLong(str[1]))
        );
      }
    }
  }

  public static class ReducerTwo extends Reducer<Text, LongWritable, Text, LongWritable> {
    public void reduce(Text key, Iterable<LongWritable> values, Context context)
        throws IOException, InterruptedException {
      long triangleCount = 0;
      boolean hasSingleEdge = false;

      for (LongWritable node : values) {
        if (node == SINGLE_EDGE) {
          hasSingleEdge = true;
        } else {
          triangleCount += 1;
        }
      }

      if (hasSingleEdge) {
        context.write(
          RESULT_KEY,
          new LongWritable(triangleCount)
        );
      }
    }
  }

  public static class ReducerThree extends Reducer<Text, LongWritable, LongWritable, NullWritable> {
    public void reduce(Text key, Iterable<LongWritable> values, Context context)
        throws IOException, InterruptedException {
      long sum = 0;
      for (LongWritable triangleCount : values) {
        sum += triangleCount.get();
      }
      context.write(new LongWritable(sum), NullWritable.get());
    }
  }

  public int run(String[] args) throws Exception {
    /**
     * Job One
     */
    Job jobOne = new Job(getConf());
    jobOne.setJobName("mapreduce-one");

    jobOne.setMapOutputKeyClass(LongWritable.class);
    jobOne.setMapOutputValueClass(LongWritable.class);

    jobOne.setOutputKeyClass(Text.class);
    jobOne.setOutputValueClass(Text.class);

    jobOne.setJarByClass(TriangleCount.class);
    jobOne.setMapperClass(MapperOne.class);
    jobOne.setReducerClass(ReducerOne.class);

    FileInputFormat.addInputPath(jobOne, new Path(args[0]));
    FileOutputFormat.setOutputPath(jobOne, new Path("/user/rayandrew/temp/mapreduce-one"));

    /**
     * Job Two
     */
    Job jobTwo = new Job(getConf());
    jobTwo.setJobName("mapreduce-two");

    jobTwo.setMapOutputKeyClass(Text.class);
    jobTwo.setMapOutputValueClass(LongWritable.class);

    jobTwo.setOutputKeyClass(LongWritable.class);
    jobTwo.setOutputValueClass(LongWritable.class);

    jobTwo.setJarByClass(TriangleCount.class);
    jobTwo.setMapperClass(MapperTextLongWritable.class);
    jobTwo.setReducerClass(ReducerTwo.class);

    FileInputFormat.addInputPath(jobTwo, new Path("/user/rayandrew/temp/mapreduce-one"));
    FileOutputFormat.setOutputPath(jobTwo, new Path("/user/rayandrew/temp/mapreduce-two"));

    /**
     * Job Three
     */
    Job jobThree = new Job(getConf());
    jobThree.setJobName("mapreduce-three");
    jobThree.setNumReduceTasks(1);

    jobThree.setMapOutputKeyClass(Text.class);
    jobThree.setMapOutputValueClass(LongWritable.class);

    jobThree.setOutputKeyClass(LongWritable.class);
    jobThree.setOutputValueClass(NullWritable.class);

    jobThree.setJarByClass(TriangleCount.class);
    jobThree.setMapperClass(MapperTextLongWritable.class);
    jobThree.setReducerClass(ReducerThree.class);

    FileInputFormat.addInputPath(jobThree, new Path("/user/rayandrew/temp/mapreduce-two"));
    FileOutputFormat.setOutputPath(jobThree, new Path(args[1]));

    int ret = jobOne.waitForCompletion(true) ? 0 : 1;
    if (ret == 0)
      ret = jobTwo.waitForCompletion(true) ? 0 : 1;
    if (ret == 0)
      ret = jobThree.waitForCompletion(true) ? 0 : 1;

    return ret;
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new TriangleCount(), args);
    System.exit(res);
  }
}