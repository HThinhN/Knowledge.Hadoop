package ex_1;

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

public class Ex_1 {

  public static class GraphMapper extends Mapper<Object, Text, Text, Text>{

    private GraphSolver graph;
    protected void setup(Context context) throws IOException, InterruptedException{
      graph = new GraphSolver();
    }
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      String line = value.toString();
      String[] vertice = line.split(" ");

      String src = vertice[0];
      String des = vertice[1];

      graph.addVertex(src);
      graph.addVertex(des);
      graph.addEdge(src,des);


    }
    protected void cleanup(Context context) throws IOException, InterruptedException {
      for (String v: graph.getAllVertice()){
          String type = graph.getType_Vertex(v);
          context.write(new Text(v), new Text(type));
        }
    }
  }

  public static class GraphReducer extends Reducer<Text,Text,Text,Text> {
    private Text value = new Text();

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
      for (Text type: values){
        value.set(type);
        break;
      }
      context.write(key,value);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "Ex_1");
    job.setJarByClass(Ex_1.class);
    job.setMapperClass(GraphMapper.class);
    job.setCombinerClass(GraphReducer.class);
    job.setReducerClass(GraphReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}

