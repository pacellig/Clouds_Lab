package fr.eurecom.dsg.mapreduce;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
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


public class Stripes extends Configured implements Tool {

  private int numReducers;
  private Path inputPath;
  private Path outputDir;

  @Override
  public int run(String[] args) throws Exception {

      Configuration conf = this.getConf();
      Job job = new Job(conf, "Co-occurrence Stripes"); // TODO: define new job instead of null using conf

      numReducers = Integer.parseInt(args[0]); // initialize numReducers

      // TODO: set job input format
      job.setInputFormatClass(TextInputFormat.class);

      // TODO: set map class and the map output key and value classes
      job.setMapperClass(StripesMapper.class);
      job.setMapOutputKeyClass(Text.class);
      job.setMapOutputValueClass(StringToIntMapWritable.class);

      /*// * TODO: set the combiner class and the combiner output key and value classes
      job.setCombinerClass(PairReducer.class); // as it is the same
*/
      // TODO: set reduce class and the reduce output key and value classes
      job.setReducerClass(StripesReducer.class);
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(StringToIntMapWritable.class);

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

  public Stripes (String[] args) {
    if (args.length != 3) {
      System.out.println("Usage: Stripes <num_reducers> <input_path> <output_path>");
      System.exit(0);
    }
    this.numReducers = Integer.parseInt(args[0]);
    this.inputPath = new Path(args[1]);
    this.outputDir = new Path(args[2]);
  }
  
  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new Stripes(args), args);
    System.exit(res);
  }
}

class StripesMapper
extends Mapper<LongWritable,   // TODO: change Object to input key type
               Text,   // TODO: change Object to input value type
               Text,   // TODO: change Object to output key type
               StringToIntMapWritable> { // TODO: change Object to output value type

    Map<String, StringToIntMapWritable> outputMap;

    @Override
    protected void setup(Context context) {
        outputMap = new HashMap<>();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] a=line.split("\\W+");

        int j=0;
        for (int i=0; i<a.length;i++) {
            // TODO it is not correct, rm the if
            /*if ( outputMap.containsKey(a[i])){ //TODO just a try, might be WRONG!!!
                continue; // TODO anyway it seems to be working
            } // TODO */
            StringToIntMapWritable array=null; // added null for efficency
            for (j=0;j<a.length;j++) {
                if (!a[i].equals(a[j])) {
                    if ( (array=outputMap.get(a[i])) == null  ){ // take the array associated to the word i
                        array = new StringToIntMapWritable();
                        array.addValue(a[j], 1);
                    } else {
                        Integer x = array.get(a[j]);
                        if ( x != null ) {
                            array.addValue(a[j], array.get(a[j]) + 1);
                        } else {
                            array.addValue(a[j], 1);
                        }
                    }
                    outputMap.put(a[i],array); // in any case, add it to the map to be outputed
                }
            }
            if (array != null){array.clear();} // added for try, it resulted in a better perfomance of the algorithm
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        Iterable<Map.Entry<String, StringToIntMapWritable>> entries = outputMap.entrySet();
        for(Map.Entry<String, StringToIntMapWritable> entry : entries) {
            String actualKey = entry.getKey();
            StringToIntMapWritable array = entry.getValue();
            context.write(new Text(actualKey), array);
        }
        outputMap.clear();
    }
}

class StripesReducer
extends Reducer<Text,   // TODO: change Object to input key type
                StringToIntMapWritable,   // TODO: change Object to input value type
                Text,   // TODO: change Object to output key type
                StringToIntMapWritable> { // TODO: change Object to output value type

    private StringToIntMapWritable outputmap = new StringToIntMapWritable();

    @Override
  public void reduce(Text key, // TODO: change Object to input key type
                     Iterable<StringToIntMapWritable> values, // TODO: change Object to input value type
                     Context context) throws IOException, InterruptedException {

    // TODO: implement the reduce method

    for ( StringToIntMapWritable v : values){
        for (Map.Entry<String,Integer> x : v.entrySet() ){
            Integer sum=outputmap.get(x.getKey());
            if ( sum == null ) {
                outputmap.addValue(x.getKey(),x.getValue());
            } else {
                outputmap.addValue(x.getKey(),sum + x.getValue());
            }
        }
    }
        // write results
    context.write(key,outputmap);
        outputmap.clear();
  }
}