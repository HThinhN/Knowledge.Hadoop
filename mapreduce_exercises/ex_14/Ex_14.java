package ex_14;

import java.io.IOException;
import java.util.StringTokenizer;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Ex_14 {

  public static class MaxMinMapper extends Mapper<Object, Text, Text, Text>{

    private Map<String,Integer> wordcount_line;
    private Text word = new Text();
    private Text count = new Text();

    protected void setup(Context context) throws IOException, InterruptedException {
        wordcount_line = new HashMap<>();
    }

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] line = value.toString().split(" ");

        for (String word: line){
            if (wordcount_line.containsKey(word)){
                wordcount_line.put(word,wordcount_line.get(word) + 1);
            }
            else wordcount_line.put(word,1);
        }
    }

    protected void cleanup(Context context) throws IOException, InterruptedException {
        for (Map.Entry<String,Integer> entry: wordcount_line.entrySet()){
            word.set(entry.getKey());
            count.set(entry.getValue().toString());
            context.write(word,count);
        }
    }

  }

  public static class MaxMinReducer extends Reducer<Text, Text, Text, Text> {

    private Text result = new Text();

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        int max = -1;
        int min = 100;
        for (Text value: values){
            int temp = Integer.parseInt(value.toString().split(" ")[0]);
            // ??? 
            if (temp > max) max = temp;
            if (temp < min) min = temp;
        }
        String max_min = String.format("%d %d",max,min);
        result.set(max_min);
        context.write(key,result);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "Max Min Occurences Program");
    job.setJarByClass(Ex_14.class);
    job.setMapperClass(MaxMinMapper.class);
    job.setCombinerClass(MaxMinReducer.class);
    job.setReducerClass(MaxMinReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPaths(job, args[0] + "," + args[1] + "," + args[2]);
    FileOutputFormat.setOutputPath(job, new Path(args[3]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}