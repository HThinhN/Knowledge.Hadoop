package ex_7;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Ex_7 {

  public static class AvgSalaryMapper extends Mapper<Object, Text, Text, FloatWritable>{

    private Text department = new Text();
    private FloatWritable salary = new FloatWritable();
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] depart_salary = line.split(" ");
        department.set(depart_salary[0]);
        salary.set(Float.parseFloat(depart_salary[1]));
        context.write(department,salary);
    }
  }

  public static class AvgSalaryReducer extends Reducer<Text, FloatWritable,Text, FloatWritable> {

    private FloatWritable avgSalary = new FloatWritable();
    
    public void reduce(Text key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException {
      float sum = 0;
      float count = 0;
      for (FloatWritable val : values) {
        sum += val.get();
        count++;
      }
      float avgsalary = sum/count;
      avgSalary.set(avgsalary);
      context.write(key, avgSalary);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "AverageSalary Program");
    job.setJarByClass(Ex_7.class);
    job.setMapperClass(AvgSalaryMapper.class);
    job.setCombinerClass(AvgSalaryReducer.class);
    job.setReducerClass(AvgSalaryReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(FloatWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}