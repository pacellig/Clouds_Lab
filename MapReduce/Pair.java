package fr.eurecom.dsg.mapreduce;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class Pair extends Configured implements Tool {

    public static class PairMapper
   extends Mapper<LongWritable, // TODO: change Object to input key type
                  Text, // TODO: change Object to input value type
                  TextPair, // TODO: change Object to output key type
                  IntWritable> { // TODO: change Object to output value type
    // TODO: implement mapper

        private IntWritable outputValue = new IntWritable(1);
        private TextPair textPair = new TextPair();

        @Override
        protected void map(LongWritable key, // TODO: change Object to input key type
                           Text value, // TODO: change Object to input value type
                           Context context) throws IOException, InterruptedException {

            // TODO: implement the map method (use context.write to emit results)
            String line = value.toString();
            String[] a=line.split("\\W+");

            int j=0;
            for (int i=0; i<a.length;i++) {
                for (j=0;j<a.length;j++) {
                    if (!a[i].equals(a[j])) {
                        textPair.set(new Text(a[i]), new Text(a[j]));
                        context.write(textPair, outputValue);
                    }
                }
            }

        }
  }

  public static class PairReducer
    extends Reducer<TextPair, // TODO: change Object to input key type
                    IntWritable, // TODO: change Object to input value type
                    TextPair, // TODO: change Object to output key type
                    IntWritable> { // TODO: change Object to output value type
    // TODO: implement reducer
    @Override
    protected void reduce(TextPair word, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {

        int sum = 0;
        for (IntWritable value : values) {
            sum += value.get();
        }
        context.write(word, new IntWritable(sum));
    }
  }

  private int numReducers;
  private Path inputPath;
  private Path outputDir;

  public Pair(String[] args) {
    if (args.length != 3) {
      System.out.println("Usage: Pair <num_reducers> <input_path> <output_path>");
      System.exit(0);
    }
    this.numReducers = Integer.parseInt(args[0]);
    this.inputPath = new Path(args[1]);
    this.outputDir = new Path(args[2]);
  }
  

  @Override
  public int run(String[] args) throws Exception {

      Configuration conf = this.getConf();
      Job job = new Job(conf, "Co-occurrence Pairs"); // TODO: define new job instead of null using conf

      numReducers = Integer.parseInt(args[0]); // initialize numReducers

      // TODO: set job input format
      job.setInputFormatClass(TextInputFormat.class);

      // TODO: set map class and the map output key and value classes
      job.setMapperClass(PairMapper.class);
      job.setMapOutputKeyClass(TextPair.class);
      job.setMapOutputValueClass(IntWritable.class);

      /*// * TODO: set the combiner class and the combiner output key and value classes
      job.setCombinerClass(PairReducer.class); // as it is the same
*/
      // TODO: set reduce class and the reduce output key and value classes
      job.setReducerClass(PairReducer.class);
      job.setOutputKeyClass(TextPair.class);
      job.setOutputValueClass(IntWritable.class);

      // TODO: set job output format
      job.setOutputFormatClass(TextOutputFormat.class);

      // TODO: add the input file as job input (from HDFS) to the variable
      //       inputPath
      FileInputFormat.addInputPath(job, new Path(args[1]));

      // TODO: set the output path for the job results (to HDFS) to the variable
      //       outputPath
      FileOutputFormat.setOutputPath(job, new Path(args[2]));

      // TODO: set the number of reducers using variable numberReducers
      job.setNumReduceTasks(numReducers);

      // TODO: set the jar class
      job.setJarByClass(WordCount.class);

      return job.waitForCompletion(true) ? 0 : 1; // this will execute the job
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new Pair(args), args);
    System.exit(res);
  }
}
