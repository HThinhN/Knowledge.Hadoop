package ex_15;

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

public class Ex_15 {

  public static class Size_UniqueMapper extends Mapper<Object, Text, IntWritable, IntWritable>{

    private final static IntWritable one = new IntWritable(1);
    private List<String> unique_words;

    protected void setup(Context context) throws IOException, InterruptedException {
        this.unique_words = new ArrayList<String>();
    }

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] line = value.toString().split(" ");

        for (String word: line){
            
            if (!unique_words.contains(word)){
                unique_words.add(word);
                IntWritable wordsize = new IntWritable(word.length());
                context.write(wordsize,one);
            }
        }
    }

  }

  public static class Size_UniqueReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {

    private IntWritable result = new IntWritable();

    public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable value: values){
            sum+= value.get();
        }
        result.set(sum);
        context.write(key,result);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "WordSizeUniqueWordCount Program");
    job.setJarByClass(Ex_15.class);
    job.setMapperClass(Size_UniqueMapper.class);
    job.setCombinerClass(Size_UniqueReducer.class);
    job.setReducerClass(Size_UniqueReducer.class);
    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(IntWritable.class);
    // FileInputFormat.addInputPaths(job, args[0] + "," + args[1] + "," + args[2]);
    // FileOutputFormat.setOutputPath(job, new Path(args[3]));
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}