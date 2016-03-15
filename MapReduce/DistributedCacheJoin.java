package fr.eurecom.dsg.mapreduce;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.FileReader;
import java.util.Set;
import java.util.HashSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class DistributedCacheJoin extends Configured implements Tool {

  private Path outputDir;
  private Path inputFile;
  private Path inputTinyFile;
  private int numReducers;

  public DistributedCacheJoin(String[] args) {
    if (args.length != 4) {
      System.out.println("Usage: DistributedCacheJoin <num_reducers> " +
      		"<input_tiny_file> <input_file> <output_dir>");
      System.exit(0);
    }
    this.numReducers = Integer.parseInt(args[0]);
    this.inputTinyFile = new Path(args[1]); // should contain the file with words to be excluded
    this.inputFile = new Path(args[2]);
    this.outputDir = new Path(args[3]);
  }

  @Override
  public int run(String[] args) throws Exception {
    Configuration conf = getConf();

    // TODO: add the smallFile to the distributed cache

    Job job = new Job(conf, "Distributed Cache Join"); // TODO: define new job instead of null using conf e setting

    DistributedCache.addCacheFile(inputTinyFile.toUri(), job.getConfiguration());

    // TODO: set job input format
    job.setInputFormatClass(TextInputFormat.class);
    // TODO: set map class and the map output key and value classes
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(IntWritable.class);
    job.setMapperClass(DMapper.class);

    // TODO: set reduce class and the reduce output key and value classes
    job.setReducerClass(DReducer.class);

    // TODO: set job output format
    job.setOutputFormatClass(TextOutputFormat.class)
    ;
    // TODO: add the input file as job input (from HDFS) to the variable
    // inputFile
    TextInputFormat.setInputPaths(job,inputFile);
    // TODO: set the output path for the job results (to HDFS) to the variable
    // outputPath
    TextOutputFormat.setOutputPath(job,outputDir);
    // TODO: set the number of reducers using variable numberReducers
    numReducers = Integer.parseInt(args[0]);
    // TODO: set the jar class
    job.setJarByClass(DistributedCacheJoin.class);

    return job.waitForCompletion(true) ? 0 : 1;
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(),
                             new DistributedCacheJoin(args),
                             args);
    System.exit(res);
  }

}

// TODO: implement mapper

class DMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

  Set<String> blackList = new HashSet<String>();
  IntWritable ONE = new IntWritable(1);
  private Text outputString = new Text();

  @Override
  protected void setup(Context context)  throws IOException, InterruptedException { // add the words to be excluded, by taking them from the input tiny
    super.setup(context);
    Path[] cacheFiles = DistributedCache.getLocalCacheFiles(context.getConfiguration());
    for (Path p: cacheFiles) {
      FileReader fr = new FileReader(new File(p.toString()));
      BufferedReader br = new BufferedReader(fr);
      String line;
      try {
        while ( (line = br.readLine()) != null) {
          line = line.trim();
          blackList.add(line);
        }
      } finally {
        br.close();
      }
    }
  }

  @Override
  protected void map(LongWritable key, Text value, Context context)
          throws IOException, InterruptedException {
    /*
    for (String word: value.toString().split("\\s+")) {
      word = word.toLowerCase().replaceAll("[\\(\\),;\\.:\"\'“”—’]", "");
      if (!ignorePatterns.contains(word)) {
        context.write(new Text(word), ONE);
      }
    }
    */
    String line = value.toString();
    String words[] = line.split("\\W+");
    for ( String word : words ){ // easy method, for each word, just emit the key as string and the count ( 1 )
      if ( !blackList.contains(word) ){ // this time add only if words is not in the blacklist
        outputString.set(word);
        context.write(outputString, ONE);
      }
    }
  }


}

// TODO: implement reducer


class DReducer extends Reducer<Text, IntWritable, Text, LongWritable> {

  private LongWritable outputSum = new LongWritable();

  @Override
  protected void reduce(Text key, Iterable<IntWritable> values,
                        Context context) throws IOException, InterruptedException {

    long partial = 0;

    // TODO: implement the reduce method (use context.write to emit results)
    for (IntWritable value : values ){
      partial += value.get();
    }
    outputSum.set(partial);
    context.write(key,outputSum);
  }
}
