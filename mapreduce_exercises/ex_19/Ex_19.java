package ex_19;

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

public class Ex_19 {

  public static class DocId_UniqueWordMapper extends Mapper<Object, Text, Text,IntWritable >{

    private Text docid = new Text();
    private IntWritable count = new IntWritable();
    private Map<String,Set<String>> docid_uwords;
    private Set<String> uwords;

    protected void setup(Context context){
      this.docid_uwords = new HashMap<>();
      this.uwords = new HashSet<>();
    }
    
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      String[] line = value.toString().split("    ");
      String[] fields = line[1].split(" ");

      String doc_id = line[0];
      
      for (String w: fields){
          uwords.add(w);
          docid_uwords.put(doc_id,uwords);
      }
    }

    protected void cleanup(Context context) throws IOException, InterruptedException{
      for (Map.Entry<String,Set<String>> entry: docid_uwords.entrySet()){
        docid.set(new Text(entry.getKey()));
        count.set(entry.getValue().size());
        context.write(docid,count);
      }
    }
  }

  public static class DocId_UniqueWordReducer extends Reducer<Text, IntWritable , Text, IntWritable > {

    public void reduce(Text key, IntWritable value, Context context) throws IOException, InterruptedException {

      context.write(key,value);  

    }

  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "DocId UniqueWord Program");
    job.setJarByClass(Ex_19.class);
    job.setMapperClass(DocId_UniqueWordMapper.class);
    job.setCombinerClass(DocId_UniqueWordReducer.class);
    job.setReducerClass(DocId_UniqueWordReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPaths(job, args[0] + "," + args[1] + "," + args[2]);
    FileOutputFormat.setOutputPath(job, new Path(args[3]));
    // FileInputFormat.addInputPath(job, new Path(args[0]));
    // FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}