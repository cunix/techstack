package com.wincent.techtack.hadoop.hdfs.hadooprpc.service;

import com.wincent.techtack.hadoop.hdfs.hadooprpc.protocol.IUserLoginService;

public class UserLoginServiceImpl implements IUserLoginService{

	public String login(String name, String passwd) {
		return name + "logged in successfully...";
	}
	
	
	

}
