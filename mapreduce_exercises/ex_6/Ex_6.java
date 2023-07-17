package ex_6;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Ex_6 {

  public static class TemperatureMapper extends Mapper<Object, Text, IntWritable, IntWritable>{

    private IntWritable temp = new IntWritable();
    private IntWritable year = new IntWritable();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] year_temp = line.split(" ");
        year.set(Integer.parseInt(year_temp[0]));
        temp.set(Integer.parseInt(year_temp[1]));
        context.write(year,temp);
    }
  }

  public static class TemperatureReducer extends Reducer<IntWritable,IntWritable,IntWritable,IntWritable> {

    private IntWritable maxTemp = new IntWritable();
    
    public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
      int maxtemp = 0;
      for (IntWritable val : values) {
        if (val.get() > maxtemp) maxtemp = val.get();
      }
      maxTemp.set(maxtemp);
      context.write(key, maxTemp);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "MaxTemp Program");
    job.setJarByClass(Ex_6.class);
    job.setMapperClass(TemperatureMapper.class);
    job.setCombinerClass(TemperatureReducer.class);
    job.setReducerClass(TemperatureReducer.class);
    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}