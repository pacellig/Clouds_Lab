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

/**
 * Word Count example of MapReduce job. Given a plain text in input, this job
 * counts how many occurrences of each word there are in that text and writes
 * the result on HDFS.
 *
 */
public class WordCountIMC extends Configured implements Tool {

    private int numReducers;
    private Path inputPath;
    private Path outputDir;

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = this.getConf();

        Job job = new Job(conf, "Word Count In-Memory Combiner");

        job.setInputFormatClass(TextInputFormat.class);

        job.setMapperClass(WCIMCMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setReducerClass(WCIMCReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputDir);

        job.setNumReduceTasks(numReducers);

        job.setJarByClass(WordCountIMC.class);

        return job.waitForCompletion(true) ? 0 : 1; // this will execute the job
    }

    public WordCountIMC (String[] args) {
        if (args.length != 3) {
            System.out.println("Usage: WordCountIMC <num_reducers> <input_path> <output_path>");
            System.exit(0);
        }
        this.numReducers = Integer.parseInt(args[0]);
        this.inputPath = new Path(args[1]);
        this.outputDir = new Path(args[2]);
    }

    public static void main(String args[]) throws Exception {
        int res = ToolRunner.run(new Configuration(), new WordCountIMC(args), args);
        System.exit(res);
    }
}

class WCIMCMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private Map<String, Integer> outputMap;

    @Override
    protected void setup(Context context) {
        outputMap = new HashMap<>();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        // the in-memory combiner technique
        StringTokenizer line = new StringTokenizer(value.toString());
        while(line.hasMoreTokens()) {
            String word = line.nextToken();
            if(outputMap.containsKey(word)) {
                int sum = outputMap.get(word) + 1;
                outputMap.put(word, sum);
            } else {
                outputMap.put(word, 1);
            }
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        Iterable<Map.Entry<String, Integer>> entries = outputMap.entrySet();
        for(Map.Entry<String, Integer> entry : entries) {
            String actualKey = entry.getKey();
            int sum = entry.getValue();
            context.write(new Text(actualKey), new IntWritable(sum));
        }
    }
}

class WCIMCReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {

        int partial = 0;
        for (IntWritable value : values)
            partial += value.get();

        context.write(key, new IntWritable(partial));
    }
}