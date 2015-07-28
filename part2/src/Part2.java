/*sampleIDsam
  Code copied from https://github.com/facebookarchive/hadoop-20/blob/master/src/examples/org/apache/hadoop/examples/WordCount.java
*/

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Part2 {

  public static class TokenizerMapper 
       extends Mapper<Object, Text, IntWritable, Text>{
    
    private Text isCancer = new Text();
    private IntWritable gID = new IntWritable();  

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString(), ",");
      
      String sampleID = itr.nextToken();
      //sID.set(sampleID);

      double normal = 0.5;
      int sum = 0;
      int counter = 0;

      while (itr.hasMoreTokens()) {
	double num = Double.parseDouble(itr.nextToken());
	isCancer.set("0");
	if( num > normal ) {
	   isCancer.set("1");
	}
	counter++;
	gID.set(counter);
	context.write(gID, isCancer);
      }

    }
  }
  
  public static class GeneCombiner 
       extends Reducer<IntWritable,Text,IntWritable,Text> {
    private Text result = new Text();
                                                                  
    public void reduce(IntWritable key, Iterable<Text> values, 
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

  public static class GeneReducer 
       extends Reducer<IntWritable,Text,Text,DoubleWritable> {
    private DoubleWritable result = new DoubleWritable();
    private Text newKey = new Text();

    public void reduce(IntWritable key, Iterable<Text> values, 
                       Context context
                       ) throws IOException, InterruptedException {

      double sum = 0.0;
      int total = 0;
      for (Text val : values) {
	String parsed = val.toString();
	System.out.println( "Sum/Total: " + parsed );
	String[] list = parsed.split("/");
	sum += Integer.parseInt(list[0]);
	total += Integer.parseInt(list[1]);
      }
      
      result.set( sum/total );
      newKey.set("gene_"+key.toString());
      context.write(newKey, result);
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
    Job job = new Job(conf, "gene score count");
    job.setJarByClass(Part2.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(GeneCombiner.class);
    job.setReducerClass(GeneReducer.class);
    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
