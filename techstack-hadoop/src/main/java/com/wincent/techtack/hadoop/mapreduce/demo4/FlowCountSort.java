package com.wincent.techtack.hadoop.mapreduce.demo4;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.wincent.techtack.hadoop.mapreduce.demo3.FlowBean;


public class FlowCountSort {

	static class FlowCountMapper extends Mapper<LongWritable, Text, Text, FlowBean>{
		
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] line = value.toString().split("\t");
			String phone = line[1];
			long upFlow=Long.parseLong(line[line.length-3]);
			long downFlow=Long.parseLong(line[line.length-2]);
			context.write(new Text(phone), new FlowBean(upFlow, downFlow));
		}
	}
	
	
	static class FlowCountReducer extends Reducer< Text, FlowBean,  Text, FlowBean>{
		@Override
		protected void reduce(Text key, Iterable<FlowBean> values, Context ctx)
				throws IOException, InterruptedException {
			
			long sumUpFlow=0;
			long sumDownFlow=0;
			for (FlowBean flowBean : values) {
				sumUpFlow+=flowBean.getUpFlow();
				sumDownFlow+=flowBean.getDownFlow();
			}
			ctx.write(key, new FlowBean(sumUpFlow, sumDownFlow));
		}
	}
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		  Job job = Job.getInstance(conf);
		  job.setJarByClass(FlowCountSort.class);
		  job.setMapperClass(FlowCountMapper.class);
		  job.setReducerClass(FlowCountReducer.class);
		  job.setMapOutputKeyClass(Text.class);
		  job.setMapOutputValueClass(FlowBean.class);
		  job.setOutputKeyClass(Text.class);
		  job.setOutputValueClass(FlowBean.class);
		  FileInputFormat.setInputPaths(job, new Path(args[0]));
		  FileOutputFormat.setOutputPath(job, new Path(args[1]));
		  boolean resp = job.waitForCompletion(true);
		  System.exit(resp?0:1);
	}
}
