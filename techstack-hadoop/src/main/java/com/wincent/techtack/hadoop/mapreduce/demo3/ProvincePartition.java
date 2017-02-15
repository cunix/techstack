package com.wincent.techtack.hadoop.mapreduce.demo3;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * k2:map
 * v2:
 * @author user
 *
 */
public class ProvincePartition extends Partitioner<Text, FlowBean>{
	
	public static Map<String,Integer> dict=new HashMap<String,Integer>();
	static{
		dict.put("136", 0);
		dict.put("137", 1);
		dict.put("138", 2);
		dict.put("139", 3);
		
	}
	
	@Override
	public int getPartition(Text key, FlowBean value, int arg2) {
		String substring = key.toString().substring(0, 3);
		Integer h = dict.get(substring);
		return h==null?4:h;
	}


}
