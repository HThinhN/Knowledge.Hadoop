package ex_13;

import java.io.IOException;
import java.util.StringTokenizer;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Ex_13 {

  public static class AvgMapper extends Mapper<Object, Text, Text, FloatWritable>{

    private Map<String, Integer> wordcount_line;
    private Text word = new Text();
    protected void setup(Context context) throws IOException, InterruptedException {
        this.wordcount_line = new HashMap<>();
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

    protected void cleanup(Context context) throws IOException, InterruptedException{
        for (Map.Entry<String,Integer> entry: wordcount_line.entrySet()){
            word.set(entry.getKey());
            context.write(word,new FloatWritable(entry.getValue()));
        }
    } 

  }

  public static class AvgReducer extends Reducer<Text, FloatWritable, Text, FloatWritable> {

    private FloatWritable result = new FloatWritable();
    public void reduce(Text key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        int count = 0;
        for (FloatWritable value: values){
            sum += value.get();
            count++;
        }
        float avg = (float)sum/count;
        result.set(avg);
        context.write(key,result);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "Average Occurences Program");
    job.setJarByClass(Ex_13.class);
    job.setMapperClass(AvgMapper.class);
    job.setCombinerClass(AvgReducer.class);
    job.setReducerClass(AvgReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(FloatWritable.class);
    FileInputFormat.addInputPaths(job, args[0] + "," + args[1] + "," + args[2]);
    FileOutputFormat.setOutputPath(job, new Path(args[3]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}