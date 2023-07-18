package ex_10;

import java.io.IOException;
import java.util.StringTokenizer;
import java.time.Duration;
import java.time.LocalDateTime;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Ex_10 {

  public static class STDCallMapper extends Mapper<Object, Text, Text, LongWritable>{

    private Text phone_number = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] line = value.toString().split("\\|");

        int STDflag = Integer.parseInt(line[4]);
        if (STDflag == 0) return;

        phone_number.set(line[0]);

        String start_datetime = line[2];
        int start_year = Integer.parseInt(start_datetime.substring(0,4));
        int start_month = Integer.parseInt(start_datetime.substring(5,7));
        int start_date = Integer.parseInt(start_datetime.substring(8,10));
        int start_hour = Integer.parseInt(start_datetime.substring(11,13));
        int start_minute = Integer.parseInt(start_datetime.substring(14,16));
        int start_second = Integer.parseInt(start_datetime.substring(17,19));

        String end_datetime = line[3];
        int end_year = Integer.parseInt(end_datetime.substring(0,4));
        int end_month = Integer.parseInt(end_datetime.substring(5,7));
        int end_date = Integer.parseInt(end_datetime.substring(8,10));
        int end_hour = Integer.parseInt(end_datetime.substring(11,13));
        int end_minute = Integer.parseInt(end_datetime.substring(14,16));
        int end_second = Integer.parseInt(end_datetime.substring(17,19));

        LocalDateTime datetime_start = LocalDateTime.of(start_year,start_month,start_date,start_hour,start_minute,start_second);
        LocalDateTime datetime_end = LocalDateTime.of(end_year,end_month,end_date,end_hour,end_minute,end_second);

        Duration duration = Duration.between(datetime_start,datetime_end);

        long min_time = duration.toMinutes();

        context.write(phone_number, new LongWritable(min_time));
    }

  }

  public static class STDCallReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
    
    private LongWritable result = new LongWritable();

    public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        long sum_min = 0;
        for (LongWritable value: values){
            sum_min += value.get();
        }
        if (sum_min > 60){
            result.set(sum_min);
            context.write(key,result);
        }
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "Telecom Call Data Record Program");
    job.setJarByClass(Ex_10.class);
    job.setMapperClass(STDCallMapper.class);
    job.setCombinerClass(STDCallReducer.class);
    job.setReducerClass(STDCallReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(LongWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}