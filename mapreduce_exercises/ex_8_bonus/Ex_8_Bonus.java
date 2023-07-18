package ex_8_bonus;

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

public class Ex_8_Bonus {

  public static class DecryptMapper extends Mapper<Object, Text, NullWritable, Text>{

    private DecryptModule decrypt_module;
    protected void setup(Context context) throws IOException, InterruptedException{
      decrypt_module = new DecryptModule();
    }

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] line = value.toString().split(",");
        String outputValue;

        if (line[0].equals("PatientID")){
            outputValue = String.format("%s,%s,%s,%s,%s,%s,%s,%s,%s",line[0],line[1],line[2],line[3],line[4],line[5],line[6],line[7],line[8]);
        }
        else{
            outputValue = String.format("%s,%s,%s,%s,%s,%s,%s,%s,%s", decrypt_module.decrypt(line[0]),
                                                                      decrypt_module.decrypt(line[1]),
                                                                      decrypt_module.decrypt(line[2]),
                                                                      decrypt_module.decrypt(line[3]),
                                                                      decrypt_module.decrypt(line[4]),
                                                                      decrypt_module.decrypt(line[5]),
                                                                      decrypt_module.decrypt(line[6]),
                                                                      decrypt_module.decrypt(line[7]),
                                                                      decrypt_module.decrypt(line[8]));
        }

        context.write(NullWritable.get(),new Text(outputValue));
    }
  }

  public static class DecryptReducer extends Reducer<NullWritable, Text, NullWritable, Text> {
    
    public void reduce(NullWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
      for (Text value: values){
        context.write(key,value);
      }
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, " Identify HealthCare Program");
    job.setJarByClass(Ex_8_Bonus.class);
    job.setMapperClass(DecryptMapper.class);
    job.setCombinerClass(DecryptReducer.class);
    job.setReducerClass(DecryptReducer.class);
    job.setOutputKeyClass(NullWritable.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}