import java.lang.*;
import java.io.*;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.examples.SecondarySort.IntPair;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.*;

public class GraphPartition extends Configured implements Tool {
  public static final Text RESULT_KEY = new Text("triangleCount");

  public static class MapperOne extends Mapper<LongWritable, Text, Text, LongPair> {
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
      Configuration conf = context.getConfiguration();
      int p = conf.getInt("partitions", -1);

      LongPair _value = new LongPair();

      String[] str = value.toString().split("\\s+");
      if (str.length > 1) {
        long node1 = Long.parseLong(str[0]);
        long node2 = Long.parseLong(str[1]);

        // Ensure the left node index is less than the right node index
        if (node1 < node2) {
          long temp = node1;
          node1 = node2;
          node2 = temp;
        }

        long hashNode1 = node1 % p;
        long hashNode2 = node2 % p;

        for (int a = 0; a <= p - 2; a++) {
          for (int b = a + 1; b <= p - 1; b++) {
            if ((hashNode1 == a) && (hashNode2 == b) || (hashNode1 == b) && (hashNode2 == a)
                || (hashNode1 == a) && (hashNode2 == a) || (hashNode1 == b) && (hashNode2 == b)) {
              _value.set(node1, node2);
              context.write(new Text(String.valueOf((a) + ',' + String.valueOf(b))), _value);
            }
          }
        }

        if (hashNode1 != hashNode2) {
          for (int a = 0; a <= p - 3; a++) {
            for (int b = a + 1; b <= p - 2; b++) {
              for (int c = b + 1; c <= p - 1; c++) {
                if ((hashNode1 == a) && (hashNode2 == a) || (hashNode1 == a) && (hashNode2 == b)
                    || (hashNode1 == a) && (hashNode2 == c) || (hashNode1 == b) && (hashNode2 == a)
                    || (hashNode1 == b) && (hashNode2 == b) || (hashNode1 == b) && (hashNode2 == c)
                    || (hashNode1 == c) && (hashNode2 == a) || (hashNode1 == c) && (hashNode2 == b)
                    || (hashNode1 == c) && (hashNode2 == c)) {
                  _value.set(node1, node2);
                  context.write(new Text(String.valueOf((a) + ',' + String.valueOf(b)) + ',' + String.valueOf(c)),
                      _value);
                }
              }
            }
          }
        }
      }
    }
  }

  public static class ReducerOne extends Reducer<Text, LongPair, NullWritable, DoubleWritable> {
    public void reduce(Text key, Iterable<LongPair> values, Context context) throws IOException, InterruptedException {
      Configuration conf = context.getConfiguration();
      int p = conf.getInt("partitions", -1);

      Iterator<LongPair> valuesIterator = values.iterator();
      Graph graph = new Graph();

      while (valuesIterator.hasNext()) {
        LongPair e = valuesIterator.next();
        graph.addEdge(e.getFirst(), e.getSecond());
      }

      context.write(NullWritable.get(), new DoubleWritable(graph.countTrianglesWithPartition(p)));
    }
  }

  public static class MapperTwo extends Mapper<LongWritable, Text, Text, DoubleWritable> {
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
      String str = value.toString();
      double val = Double.parseDouble(str);

      if (val > 0.0) {
        context.write(RESULT_KEY, new DoubleWritable(val));
      }
    }
  }

  public static class ReducerTwo extends Reducer<Text, DoubleWritable, NullWritable, DoubleWritable> {
    public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
        throws IOException, InterruptedException {
      Iterator<DoubleWritable> valueIterator = values.iterator();
      double sum = 0.0;

      while (valueIterator.hasNext()) {
        sum += valueIterator.next().get();
      }

      context.write(NullWritable.get(), new DoubleWritable(sum));
    }
  }

  public int run(String[] args) throws Exception {
    /**
     * Job One
     */
    long startTime = System.nanoTime();
    getConf().setInt("partitions", Integer.parseInt(args[2]));

    Job jobOne = new Job(getConf());
    jobOne.setJobName("mapreduce-one");

    jobOne.setMapOutputKeyClass(Text.class);
    jobOne.setMapOutputValueClass(IntPair.class);

    jobOne.setOutputKeyClass(NullWritable.class);
    jobOne.setOutputValueClass(DoubleWritable.class);

    jobOne.setJarByClass(GraphPartition.class);
    jobOne.setMapperClass(MapperOne.class);
    jobOne.setReducerClass(ReducerOne.class);

    TextInputFormat.addInputPath(jobOne, new Path(args[0]));
    TextOutputFormat.setOutputPath(jobOne, new Path("/user/rayandrew/temp"));

    Job jobTwo = new Job(getConf());
    jobTwo.setJobName("mapreduce-two");

    jobTwo.setMapOutputKeyClass(Text.class);
    jobTwo.setMapOutputValueClass(DoubleWritable.class);

    jobTwo.setOutputKeyClass(NullWritable.class);
    jobTwo.setOutputValueClass(DoubleWritable.class);

    jobTwo.setJarByClass(GraphPartition.class);
    jobTwo.setMapperClass(MapperTwo.class);
    jobTwo.setReducerClass(ReducerTwo.class);

    TextInputFormat.addInputPath(jobTwo, new Path("/user/rayandrew/temp"));
    TextOutputFormat.setOutputPath(jobTwo, new Path(args[1]));

    int ret = jobOne.waitForCompletion(true) ? 0 : 1;
    if (ret == 0)
      ret = jobTwo.waitForCompletion(true) ? 0 : 1;

    long estimatedTime = System.nanoTime() - startTime;
    System.out.println("Estimated Execution Time = " + estimatedTime + " nanoseconds");
    System.out.println("Estimated Execution Time = " + estimatedTime / 1000000000 + " seconds");

    return ret;
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new GraphPartition(), args);
    System.exit(res);
  }
}
