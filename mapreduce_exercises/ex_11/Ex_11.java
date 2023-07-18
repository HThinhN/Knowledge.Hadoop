package ex_11;

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

public class Ex_11 {

  public static class ConnectedComponentMapper extends Mapper<Object, Text, Text, IntWritable>{

    private GraphHandle graph;
    private IntWritable result = new IntWritable();

    protected void setup(Context context) throws IOException, InterruptedException {
        graph = new GraphHandle();
    }

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] line = value.toString().split(" ");

        String src = line[0];
        graph.addVertex(src);

        for (String des: line){
            if (des != src){
                graph.addVertex(des);
                graph.addEdge(src,des);
            }
        } 
    }

    protected void cleanup(Context context) throws IOException, InterruptedException {
        int num_connected_component = graph.calculateNumOfConnectedComponent();
        result.set(num_connected_component);
        context.write(new Text("Number of connected component in graph"), result);
    }

  }

  public static class ConnectedComponentReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    
    public void reduce(Text key, IntWritable value, Context context) throws IOException, InterruptedException {
       context.write(key,value);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "Connected Component Program");
    job.setJarByClass(Ex_11.class);
    job.setMapperClass(ConnectedComponentMapper.class);
    job.setCombinerClass(ConnectedComponentReducer.class);
    job.setReducerClass(ConnectedComponentReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}