package ex_8;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Ex_8 {

  public static class EncryptMapper extends Mapper<Object, Text, NullWritable, Text>{

    private EncryptModule encrypt_module;
    protected void setup(Context context) throws IOException, InterruptedException{
      encrypt_module = new EncryptModule();
    }

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] line = value.toString().split(",");
        String outputValue;

        if (line[0].equals("PatientID")){
            outputValue = String.format("%s,%s,%s,%s,%s,%s,%s,%s,%s",line[0],line[1],line[2],line[3],line[4],line[5],line[6],line[7],line[8]);
        }
        else{
            outputValue = String.format("%s,%s,%s,%s,%s,%s,%s,%s,%s", encrypt_module.encrypt(line[0]),
                                                                      encrypt_module.encrypt(line[1]),
                                                                      encrypt_module.encrypt(line[2]),
                                                                      encrypt_module.encrypt(line[3]),
                                                                      encrypt_module.encrypt(line[4]),
                                                                      encrypt_module.encrypt(line[5]),
                                                                      encrypt_module.encrypt(line[6]),
                                                                      encrypt_module.encrypt(line[7]),
                                                                      encrypt_module.encrypt(line[8]));
        }

        context.write(NullWritable.get(),new Text(outputValue));
    }
  }

  public static class EncryptReducer extends Reducer<NullWritable, Text, NullWritable, Text> {
    
    public void reduce(NullWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
      for (Text value: values){
        context.write(key,value);
      }
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, " De Identify HealthCare Program");
    job.setJarByClass(Ex_8.class);
    job.setMapperClass(EncryptMapper.class);
    job.setCombinerClass(EncryptReducer.class);
    job.setReducerClass(EncryptReducer.class);
    job.setOutputKeyClass(NullWritable.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}