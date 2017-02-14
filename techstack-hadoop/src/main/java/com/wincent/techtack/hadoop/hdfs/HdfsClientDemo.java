package com.wincent.techtack.hadoop.hdfs;


import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;

public class HdfsClientDemo {

	FileSystem fs =null;
	
	@Before
	public void init() throws Exception{
		Configuration conf = new Configuration();
		//获取远程hdfs文件系统
    	conf.set("fs.defaultFS", "192.168.244.100:9000");
		//拿到一个文件系统操作的客户端实例对象
		fs = FileSystem.get(new URI("hdfs://192.168.244.100:9000"), conf, "hadoop");
	}
	
	@Test
	public void testUpload() throws Exception{
		fs.copyFromLocalFile(new Path("D:\\tmp\\myfml.mp3"),new Path("/"));
		fs.close();
	}
	
	@Test
	public void testDownLoad() throws Exception{
		fs.copyToLocalFile(new Path("/hello.mp3"),new Path("D:\\tmp\\1.mp3"));
		fs.close();
	}
	
	
	public static void main(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "192.168.244.100:9000");
		FileSystem fs = FileSystem.get(conf);
		fs.copyFromLocalFile(new Path("D:\\tmp\\hello.mp3"),new Path("/hello.mp3"));
		fs.close();
		
	}
	
}
