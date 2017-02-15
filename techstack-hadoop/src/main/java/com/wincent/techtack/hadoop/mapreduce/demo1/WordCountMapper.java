package com.wincent.techtack.hadoop.mapreduce.demo1;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * KEY-
 * @author user
 *
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable>{

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String[] split = value.toString().split(" ");
		for (String string : split) {
			context.write(new Text(string),new IntWritable(1));
		}
	}
}

