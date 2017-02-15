package com.wincent.techtack.hadoop.mapreduce.demo1;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable>{

	/**
	 * key :组名
	 * values
	 * <group1,1><group1,1><group1,1><group1,1><group1,1><group1,1>
	 * <group2,1><group2,1><group2,1><group2,1><group2,1><group2,1>
	 */
	@Override
	protected void reduce(Text key, Iterable<IntWritable> values,
			Context ctx) throws IOException, InterruptedException {
		int count=0;
		for (IntWritable intWritable : values) {
			count+=intWritable.get();
		}
		ctx.write(key, new IntWritable(count));
	}
	
}
