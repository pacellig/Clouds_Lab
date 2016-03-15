package fr.eurecom.dsg.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;


public class OrderInversion extends Configured implements Tool {

  private final static String ASTERISK = "\0";

  public static class PartitionerTextPair extends
  Partitioner<TextPair, IntWritable> {
    @Override
    public int getPartition(TextPair key, IntWritable value,
        int numPartitions) {
      // TODO: implement getPartition such that pairs with the same first element
      //       will go to the same reducer. You can use toUnsighed as utility.
      int hc = key.hashCode();
      return (toUnsigned(hc))%numPartitions;
    }
    
    /**
     * toUnsigned(10) = 10
     * toUnsigned(-1) = 2147483647
     * 
     * @param val Value to convert
     * @return the unsigned number with the same bits of val 
     * */
    public static int toUnsigned(int val) {
      return val & Integer.MAX_VALUE;
    }
  }

  public static class PairMapper extends
  Mapper<LongWritable, Text, TextPair, IntWritable> {

  //  private IntWritable outputValue = new IntWritable(1);
    private TextPair textPair = new TextPair();
    private HashMap<TextPair,Integer> outputMap;

    @Override
    protected void setup(Mapper.Context context) throws IOException, InterruptedException {
      outputMap = new HashMap<TextPair,Integer>();
    }

    @Override
    public void map(LongWritable key, Text value, Context context)
    throws java.io.IOException, InterruptedException {
      // TODO: implement the map method
      String line = value.toString();
      String[] a=line.split("\\W+");

      /*for (int i=0; i<a.length;i++) {
        int count=0;
        for (int j=0;j<a.length;j++) {
          if (!a[i].equals(a[j])) {
            count++;
            textPair.set(new Text(a[i]),new Text(a[j]));
            if ( outputMap.containsKey(textPair) ){
              outputMap.put(textPair, outputMap.get(textPair) +1 );
            } else {
              outputMap.put(textPair,1);
            }
          }
        }
        // emit the asterisk with count
        textPair.set(new Text(a[i]), new Text(ASTERISK));
        outputMap.put(textPair,count);
      }*/

      for (int i=0; i<a.length;i++) {
        int count=0;
        for (int j=0;j<a.length;j++) {
          if (!a[i].equals(a[j])) {
            count++;
            textPair.set(new Text(a[i]), new Text(a[j]));
            context.write(textPair, new IntWritable(1));
          }
        }
        textPair.set(new Text(a[i]), new Text(ASTERISK));
        context.write(textPair,new IntWritable(count)); // < <word,*> , count >
      }

    }

    /*@Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
      IntWritable x = new IntWritable();
      for (Map.Entry<TextPair, Integer> entry : outputMap.entrySet()) {
        x.set(entry.getValue());
        context.write(entry.getKey(), x);
      }
      outputMap.clear();
    }*/
  }

  public static class PairReducer extends
  Reducer<TextPair, IntWritable, TextPair, DoubleWritable> {

    // TODO: implement the reduce method
    private DoubleWritable totalCount = new DoubleWritable();
    private DoubleWritable relativeCount = new DoubleWritable();
    private Text currentWord = new Text("NOT_SET");
    int count;

    @Override
    protected void reduce(TextPair key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
      //currentWord = key.getFirst();
      count=getTotalCount(values);
      if (key.getSecond().toString().equals(ASTERISK)) {
        if (key.getSecond().equals(currentWord)) {
          totalCount.set(totalCount.get() + count);
        } else {
          currentWord.set(key.getFirst());
          totalCount.set(0);
          totalCount.set(count);
        }
      } else {
       // count = getTotalCount(values);
        relativeCount.set((double) count / totalCount.get());
        context.write(key, relativeCount);
      }
    }

    private int getTotalCount(Iterable<IntWritable> values) {
      int count = 0;
      for (IntWritable value : values) {
        count += value.get();
      }
      return count;
    }

  }

  private int numReducers;
  private Path inputPath;
  private Path outputDir;

  @Override
  public int run(String[] args) throws Exception {
    Configuration conf = this.getConf();
    Job job = new Job (conf);  // TODO: define new job instead of null using conf e setting a name
    job.setJobName("OrderInversion");

    // TODO: set job input format
    job.setInputFormatClass(TextInputFormat.class);

    // TODO: set map class and the map output key and value classes
    job.setMapperClass(PairMapper.class);
    job.setMapOutputKeyClass(TextPair.class);
    job.setMapOutputValueClass(IntWritable.class);

    // set partitioner
    job.setPartitionerClass(PartitionerTextPair.class);

    // TODO: set reduce class and the reduce output key and value classes
    job.setReducerClass(PairReducer.class);
    job.setOutputKeyClass(TextPair.class);
    job.setOutputValueClass(DoubleWritable.class);

    // TODO: set job output format
    job.setOutputFormatClass(TextOutputFormat.class);

    // TODO: add the input file as job input (from HDFS) to the variable
    //       inputFile
    FileInputFormat.setInputPaths(job, inputPath);

    // TODO: set the output path for the job results (to HDFS) to the variable
    //       outputPath
    FileOutputFormat.setOutputPath(job, outputDir);
    // TODO: set the number of reducers using variable numberReducers
    job.setNumReduceTasks(numReducers);

    // TODO: set the jar class
    job.setJarByClass(OrderInversion.class);

    return job.waitForCompletion(true) ? 0 : 1;
  }

  OrderInversion(String[] args) {
    if (args.length != 3) {
      System.out.println("Usage: OrderInversion <num_reducers> <input_file> <output_dir>");
      System.exit(0);
    }
    this.numReducers = Integer.parseInt(args[0]);
    this.inputPath = new Path(args[1]);
    this.outputDir = new Path(args[2]);
  }
  
  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new OrderInversion(args), args);
    System.exit(res);
  }
}
