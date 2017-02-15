package com.wincent.techtack.hadoop.mapreduce.demo3;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class FlowBean implements Writable {

	private long upFlow;
	private long downFlow;
	private String phone;
	private long totalCount;
	
	
	
	public FlowBean(long upFlow, long downFlow) {
		super();
		this.upFlow = upFlow;
		this.downFlow = downFlow;
		this.totalCount=this.upFlow +this.downFlow;
		
	}
	public String getPhone() {
		return phone;
	}
	public void setPhone(String phone) {
		this.phone = phone;
	}
	public long getTotalCount() {
		return totalCount;
	}
	public void setTotalCount(long totalCount) {
		this.totalCount = totalCount;
	}
	public FlowBean() {
		super();
	}
	public long getUpFlow() {
		return upFlow;
	}
	public void setUpFlow(long upFlow) {
		this.upFlow = upFlow;
	}
	public long getDownFlow() {
		return downFlow;
	}
	public void setDownFlow(long downFlow) {
		this.downFlow = downFlow;
	}
	
	/**
	 * 反序列化
	 */
	public void readFields(DataInput input) throws IOException {
		upFlow = input.readLong();
		downFlow = input.readLong();
		totalCount=input.readLong();
	}
	
	/**
	 * 序列化
	 */
	public void write(DataOutput out) throws IOException {
		out.writeLong(upFlow);
		out.writeLong(downFlow);
		out.writeLong(totalCount);
	}
	@Override
	public String toString() {
		return phone+"\t"+upFlow+"\t"+downFlow+"\t"+totalCount;
	}
	
}
