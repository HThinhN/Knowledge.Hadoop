package ex_18;

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

public class Ex_18 {

  public static class DocIdMapper extends Mapper<Object, Text, Text,Text>{

    private Text docid = new Text();
    private Text word = new Text();
    
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      String[] line = value.toString().split("    ");
      docid.set(line[0]);
      String[] fields = line[1].split(" ");

      for (String w: fields){
        word.set(w);
        context.write(word,docid);
      }
      
    }
  }

  public static class DocIdReducer extends Reducer<Text, Text, Text, Text> {

    private Text result = new Text();
    
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
      // Map<String,Integer> docid_time = new HashMap<>();
      List<String> docid_list = new ArrayList<>();

      for (Text value: values){
        String doc_id = value.toString().split(" ")[0];
        docid_list.add(doc_id);
      }
      
      String main_docid = null;
      int max_times = 0;

      for (String docid: docid_list){
        int count = 0;

        for (int i = 0; i < docid_list.size(); i++) {
            if (docid_list.get(i).equals(docid)) {
                count++;
            }
        }
        if (count > max_times){
          max_times = count;
          main_docid = docid;
        } 
      }
    
      result.set(main_docid);
      context.write(key,result);
    }

  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "Word DocId Program");
    job.setJarByClass(Ex_18.class);
    job.setMapperClass(DocIdMapper.class);
    job.setCombinerClass(DocIdReducer.class);
    job.setReducerClass(DocIdReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPaths(job, args[0] + "," + args[1] + "," + args[2]);
    FileOutputFormat.setOutputPath(job, new Path(args[3]));
    // FileInputFormat.addInputPath(job, new Path(args[0]));
    // FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}