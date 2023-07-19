package ex_12;

import java.io.IOException;
import java.util.StringTokenizer;
import java.util.ArrayList;
import java.util.HashSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Ex_12 {

  public static class _2GramMapper extends Mapper<Object, Text, Text, IntWritable>{

    private Text word_2gram = new Text();
    private final static IntWritable one = new IntWritable(1);

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
       String[] line = value.toString().split(" ");
       for (int i = 0; i < line.length - 1; i++){
            word_2gram.set(line[i]+" "+line[i+1]);
            context.write(word_2gram,one);
       }
    }

  }

  public static class _2GramReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable value: values){
            sum+=value.get();
        }
        result.set(sum);
        context.write(key,result);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "2 Gram Program");
    job.setJarByClass(Ex_12.class);
    job.setMapperClass(_2GramMapper.class);
    job.setCombinerClass(_2GramReducer.class);
    job.setReducerClass(_2GramReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPaths(job, args[0] + "," + args[1] + "," + args[2]);
    FileOutputFormat.setOutputPath(job, new Path(args[3]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}