package ex_9;

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

public class Ex_9 {

  public static class TrackMapper extends Mapper<Object, Text, Text, IntWritable>{

    private final static IntWritable one = new IntWritable(1);
    private final static IntWritable zero = new IntWritable(0);
    private ArrayList<String> user_ids;

    protected void setup(Context context) throws IOException, InterruptedException {
        user_ids = new ArrayList<String>();
    }

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] line = value.toString().split("\\|");

        if (line[0].equals("UserId")) return;

        user_ids.add(line[0]);
        String user_id = line[0];

        int num_shared = Integer.parseInt(line[2]);
        if (num_shared == 1){ 
            context.write(new Text("Track shared - " + user_id),one);
        }
        else context.write(new Text("Track shared - " + user_id),zero);

        int num_listened_on_radio = Integer.parseInt(line[3]);
        if (num_listened_on_radio == 1){
            context.write(new Text("Track radio - "+ user_id),one);
        }
        else context.write(new Text("Track radio - "+ user_id),zero);

        context.write(new Text("Track total - " + user_id),one);

        int num_skipped = Integer.parseInt(line[4]);
       
        if (num_skipped == 1){
            context.write(new Text("Track skipped - "+ user_id),one);
        }
        else  context.write(new Text("Track skipped - "+ user_id),zero);
        
    }

    protected void cleanup(Context context) throws IOException, InterruptedException {
        HashSet<String> useridsset = new HashSet<String>(user_ids);
        int num_unique_listeners = useridsset.size();
        context.write(new Text("Number of unique listeners"), new IntWritable(num_unique_listeners));
    }
  }

  public static class TrackReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable value: values){
            sum+= value.get();
        }
        context.write(key, new IntWritable(sum));
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "Music Track  Program");
    job.setJarByClass(Ex_9.class);
    job.setMapperClass(TrackMapper.class);
    job.setCombinerClass(TrackReducer.class);
    job.setReducerClass(TrackReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}