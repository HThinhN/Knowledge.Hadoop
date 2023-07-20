package ex_17;

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

public class Ex_17 {

  public static class NumDocMapper extends Mapper<Object, Text, Text, IntWritable>{

    private IntWritable docid = new IntWritable();
    private Text word = new Text();
    
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      String[] line = value.toString().split("    ");
      docid.set(Integer.parseInt(line[0].substring(2,3)));
      String[] fields = line[1].split(" ");

      for (String w: fields){
        word.set(w);
        context.write(word,docid);
      }
      
    }

  }

  public static class NumDocReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
      int count = 0;
      for (IntWritable value: values){
        List<Integer> uniquedocs = new ArrayList<>();
        int doc_id = value.get();
        if (!uniquedocs.contains(doc_id)){
          uniquedocs.add(doc_id);
          count++;
        }
      }
      context.write(key,new IntWritable(count));
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "Word NumDoc Program");
    job.setJarByClass(Ex_17.class);
    job.setMapperClass(NumDocMapper.class);
    job.setCombinerClass(NumDocReducer.class);
    job.setReducerClass(NumDocReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPaths(job, args[0] + "," + args[1] + "," + args[2]);
    FileOutputFormat.setOutputPath(job, new Path(args[3]));
    // FileInputFormat.addInputPath(job, new Path(args[0]));
    // FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}