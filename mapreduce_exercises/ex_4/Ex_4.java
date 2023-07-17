package ex_4;

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

public class Ex_4 {

  public static class WeatherMapper extends Mapper<Object, Text, Text, Text>{

    private Text word = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String date = line.substring(12,14);
        String month = line.substring(10,12);
        String year = line.substring(6,10);
        float temp_Max = Float.parseFloat(line.substring(39,45).trim());
        float temp_Min = Float.parseFloat(line.substring(47,53).trim());
        if (temp_Max > 25) word.set("Hot Day");
        if (temp_Min < 15) word.set("Cold Day");

        context.write(new Text(date + "-" + month + "-" + year), word);
    }
  }

  public static class WeatherReducer extends Reducer<Text,Text,Text,Text> {
    public void reduce(Text key, Text value, Context context) throws IOException, InterruptedException {
      context.write(key, value);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "WeatherData Program");
    job.setJarByClass(Ex_4.class);
    job.setMapperClass(WeatherMapper.class);
    job.setCombinerClass(WeatherReducer.class);
    job.setReducerClass(WeatherReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}