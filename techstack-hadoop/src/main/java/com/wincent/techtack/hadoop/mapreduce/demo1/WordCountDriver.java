package com.wincent.techtack.hadoop.mapreduce.demo1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


/**
 * 封装MR运行参数
 * @author user
 */
public class WordCountDriver {

	public static void main(String[] args) throws Exception {
		
	  Configuration conf = new Configuration();
//	  conf.set("mapreduce.framework.name", "yarn");
//	  conf.set("yarn.resourcemanager.hostname", "hadoop1");
	  Job job = Job.getInstance(conf);
	  //job.setJar(jar);
	  //指定本程序的jar包所在路径
	  job.setJarByClass(WordCountDriver.class);
	  //Mapper Output Type
	  job.setMapperClass(WordCountMapper.class);
	  job.setReducerClass(WordCountReducer.class);
	  //Reducer Output Type
	  job.setMapOutputKeyClass(Text.class);
	  job.setMapOutputValueClass(IntWritable.class);
	  //Finally Output Type
	  job.setOutputKeyClass(Text.class);
	  job.setOutputValueClass(IntWritable.class);
	  //Set File Path on HDFS 
	  FileInputFormat.setInputPaths(job, new Path(args[0]));
	  FileOutputFormat.setOutputPath(job, new Path(args[1]));
	  //Submit Job To Cluster
	  boolean resp = job.waitForCompletion(true);
	  System.exit(resp?0:1);
	}
	
}
