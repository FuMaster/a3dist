/*
Code copied from https://github.com/facebookarchive/hadoop-20/blob/master/src/examples/org/apache/hadoop/examples/WordCount.java
*/

import java.io.IOException;
import java.util.StringTokenizer;
import java.io.File;
import java.util.Iterator;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
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
     
      String token = itr.nextToken();
      String sampleID = token.split("_")[1];
      int geneCounter = 0;
      while (itr.hasMoreTokens()) {
		double num = Double.parseDouble(itr.nextToken());
		geneCounter++;
		if( num > 0 ){
	   		ret.set(sampleID+":"+num);
	   		gID.set(geneCounter);
	   		context.write(gID, ret);
		}
      }

    }
  }
  
  public static class GeneReducer 
       extends Reducer<IntWritable,Text,IntWritable,Text> {
    private Text result = new Text();

    public void reduce(IntWritable key, Iterable<Text> values, 
                       Context context
                       ) throws IOException, InterruptedException {

	  String ret = "";
/*
      HashMap<Integer,String> map = new HashMap<Integer,String>();
      for (Text val : values) {
		String[] pair = val.toString().split(":");
		int intVal = Integer.parseInt(pair[0]);
		map.put(intVal,pair[1]);
      }

	  Map<Integer, String> treeMap = new TreeMap<Integer, String>(map);
*/

	  Map<Integer, String> treeMap = new TreeMap<Integer, String>();
      for (Text val : values) {
		String[] pair = val.toString().split(":");
		int intVal = Integer.parseInt(pair[0]);
		treeMap.put(intVal,pair[1]);
      }

	  for (Map.Entry<Integer, String> entry : treeMap.entrySet()) {
		int mapKey = entry.getKey();
		String value = entry.getValue();

		ret += mapKey+":"+value+",";
      }

      result.set(ret.substring(0,ret.length()-1));
	//TODO: don't need to send geneID as key
      context.write(key, result);
    }
  }

  public static class SampleMapper 
       extends Mapper<Object, Text, Text, MapWritable>{
    
    private MapWritable result = new MapWritable();
    private Text sID = new Text();  
	private ArrayList<String> list = new ArrayList<String>();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString(), ",");

      //skip geneID
      itr.nextToken();

      while (itr.hasMoreTokens()) {
		String token = itr.nextToken();
		String[] t1 = token.split(":");
		double v1 = Double.parseDouble(t1[1]);

		for (String item2 : list) {
	   		String[] t2 = item2.split(":");
	   		double xProduct = Double.parseDouble(t2[1])*v1;
	//		System.out.println("Key1: "+t1[0]+", Key2: "+t2[0]+", Value: "+xProduct);
	   		result.put( new Text(t2[0]), new DoubleWritable(xProduct));
		}

		if (!list.isEmpty()) {
			sID.set(t1[0]);
			context.write(sID, result);
		}

		list.add(token);
		result.clear();
	 }
	 list.clear();
    }
  }

  public static class SampleCombiner 
       extends Reducer<Text,MapWritable,Text,MapWritable> {
 
	private MapWritable result = new MapWritable();

   public void reduce(Text key, Iterable<MapWritable> values, 
                       Context context
                       ) throws IOException, InterruptedException {

      for (MapWritable val : values) {
	  	for (Map.Entry<Writable, Writable> entry : val.entrySet()) {
	    	Text valKey = (Text)entry.getKey();

	     	double entryVal = ((DoubleWritable)entry.getValue()).get();
	     	if(result.containsKey(valKey)) {
				entryVal += ((DoubleWritable)result.get(valKey)).get();
	     	}

	     	result.put(valKey, new DoubleWritable(entryVal));
	  	}
	  }

	  context.write(key, result);
	  result.clear();
    }
  }

  public static class SampleReducer 
       extends Reducer<Text,MapWritable,Text,DoubleWritable> {

    private Text pair = new Text();
    private DoubleWritable result = new DoubleWritable();
	private HashMap<String,Double> map = new HashMap<String,Double>();

    public void reduce(Text key, Iterable<MapWritable> values, 
                       Context context
                       ) throws IOException, InterruptedException {

      for (MapWritable val : values) {
		for (Map.Entry<Writable, Writable> entry : val.entrySet()) {
    	   	String valKey = ((Text)entry.getKey()).toString();

			double entryVal = ((DoubleWritable)entry.getValue()).get();
            if(map.containsKey(valKey)) {
                entryVal += map.get(valKey);
            }

//			System.out.println("Pair: " + key + "," + valKey);
//			System.out.println("Value: " + entryVal);
            map.put(valKey, entryVal);
        }
      }

      for (Map.Entry<String, Double> entry : map.entrySet()) {
		pair.set( "sample_"+key+",sample_"+entry.getKey() );
		result.set(entry.getValue());
		context.write(pair, result);
      }

	  map.clear();
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
    job.setReducerClass(GeneReducer.class);
    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(Text.class);

    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
    FileOutputFormat.setOutputPath(job, outputPath);

    Configuration conf2 = new Configuration();
    conf2.set("mapreduce.output.textoutputformat.separator", ",");

    ControlledJob cJob2 = new ControlledJob(conf2);
    cJob2.setJobName("Sample MR Job");

    Job sampleJob = cJob2.getJob();
    cJob2.addDependingJob( cJob1 );
    
    sampleJob.setJarByClass(Part3.class);
    sampleJob.setMapperClass(SampleMapper.class);    
    sampleJob.setCombinerClass(SampleCombiner.class);
    sampleJob.setReducerClass(SampleReducer.class);
    sampleJob.setOutputKeyClass(Text.class);
    sampleJob.setOutputValueClass(MapWritable.class);
   
    FileInputFormat.addInputPath(sampleJob, outputPath);
    FileOutputFormat.setOutputPath(sampleJob, new Path(otherArgs[1]));

   JobControl ctrl = new JobControl("Part3");
   ctrl.addJob( cJob1 );
   ctrl.addJob( cJob2 );

   Thread t = new Thread(ctrl);
   t.start();

   while (t.isAlive() && !ctrl.allFinished()) {
	Thread.sleep(10000);
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
