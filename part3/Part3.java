/*sampleIDsam
  Code copied from https://github.com/facebookarchive/hadoop-20/blob/master/src/examples/org/apache/hadoop/examples/WordCount.java
*/

import java.io.IOException;
import java.util.StringTokenizer;
import java.io.File;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Part3 {

  public static class GeneMapper 
       extends Mapper<Object, Text, IntWritable, Text>{
    
    private Text ret = new Text();
    private IntWritable gID = new IntWritable();  

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString(), ",");
      
      String sampleID = itr.nextToken();
      int counter = 0;
      int geneCounter = 0;
      while (itr.hasMoreTokens()) {
	double num = Double.parseDouble(itr.nextToken());
	geneCounter++;
	if( num > 0 ){
	   ret.set(sampleID+":"+num);
	   counter++;
	   gID.set(geneCounter);
	   context.write(gID, ret);
	}
      }

    }
  }
  
/*
  public static class GeneCombiner 
       extends Reducer<Text,Text,Text,Text> {
    private Text result = new Text();
                                                                  
    public void reduce(Text key, Iterable<Text> values, 
                       Context context
                       ) throws IOException, InterruptedException {
      int sum = 0;
      int total = 0;

      for (Text val : values) {
        sum += Integer.parseInt(val.toString());
	total++;
      }
      result.set(sum+"/"+total);
      context.write(key, result);
    }
  }
*/

  public static class GeneReducer 
       extends Reducer<IntWritable,Text,IntWritable,Text> {
    private Text result = new Text();

    public void reduce(IntWritable key, Iterable<Text> values, 
                       Context context
                       ) throws IOException, InterruptedException {

      String ret = "";
      for (Text val : values) {
	ret += val.toString() + ",";
      }
      result.set(ret.substring(0,ret.length()-1));
      context.write(key, result);
    }
  }

  public static class SampleMapper 
       extends Mapper<Object, Text, IntWritable, Text>{
    
    private Text ret = new Text();
    private IntWritable gID = new IntWritable();  

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString(), ",");
      gID.set(1);
      ret.set("Hello World");
      context.write(gID, ret);
     /* 
      String sampleID = itr.nextToken();
      int counter = 0;
      int geneCounter = 0;
      while (itr.hasMoreTokens()) {
	double num = Double.parseDouble(itr.nextToken());
	geneCounter++;
	if( num > 0 ){
	   ret.set(sampleID+":"+num);
	   counter++;
	   gID.set(geneCounter);
	   context.write(gID, ret);
	}
      }
*/

    }
  }

  public static class SampleReducer 
       extends Reducer<IntWritable,Text,IntWritable,Text> {
    private Text result = new Text();

    public void reduce(IntWritable key, Iterable<Text> values, 
                       Context context
                       ) throws IOException, InterruptedException {

      String ret = "";
      for (Text val : values) {
	ret += val.toString() + ",";
      }
      result.set(ret.substring(0,ret.length()-1));
      context.write(key, result);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    conf.set("mapreduce.output.textoutputformat.separator", ",");
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length != 2) {
      System.err.println("Usage: gene score count <in> <out>");
      System.exit(2);
    }
    Path outputPath = new Path(otherArgs[1]+"_tmp");

    ControlledJob cJob1 = new ControlledJob(conf);
    cJob1.setJobName("Gene MR Job");

    Job job = cJob1.getJob();
    job.setJarByClass(Part3.class);
    job.setMapperClass(GeneMapper.class);
//    job.setCombinerClass(GeneCombiner.class);
    job.setReducerClass(GeneReducer.class);
    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(Text.class);

    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
    FileOutputFormat.setOutputPath(job, outputPath);

    System.out.println("Starting second job");

    Configuration conf2 = new Configuration();
    conf2.set("mapreduce.output.textoutputformat.separator", ",");

    ControlledJob cJob2 = new ControlledJob(conf2);
    cJob2.setJobName("Sample MR Job");

    Job sampleJob = cJob2.getJob();
    cJob2.addDependingJob( cJob1 );
    
    sampleJob.setJarByClass(Part3.class);
    sampleJob.setMapperClass(SampleMapper.class);
    sampleJob.setReducerClass(SampleReducer.class);
    sampleJob.setOutputKeyClass(IntWritable.class);
    sampleJob.setOutputValueClass(Text.class);
   
    FileInputFormat.addInputPath(sampleJob, outputPath);
    FileOutputFormat.setOutputPath(sampleJob, new Path(otherArgs[1]));

   JobControl ctrl = new JobControl("Part3");

   ctrl.addJob( cJob1 );
   ctrl.addJob( cJob2 );

   Thread t = new Thread(ctrl);
   t.start();

   while (t.isAlive() && !ctrl.allFinished()) {
	Thread.sleep(5000);

   }

   if(t.isAlive() && !ctrl.allFinished()){
	System.out.println("Still Alive");
   }else{
	FileUtils.deleteDirectory(new File(outputPath.toString()));
	System.exit(0);
   }
 //   System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
