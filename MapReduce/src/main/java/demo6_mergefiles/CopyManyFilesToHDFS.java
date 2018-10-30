package com.hadoop.mergefiles;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;




public class CopyManyFilesToHDFS {
	private static FileSystem fs = null;
	private static FileSystem local = null;

	public static void main(String[] args) throws IOException, URISyntaxException {
		// TODO Auto-generated method stub
		list();
	}
		
	//定义一个读取配置文件的方法
	
		
	
	private static void list() throws IOException, URISyntaxException {
		// TODO Auto-generated method stub
		//读取配置文件
				Configuration conf = new Configuration(); 

				//获取默认文件系统  在Hadoop 环境下运行，也可以使用此种方法获取文件系统
				//fs = FileSystem.get(conf);
				
				//获取指定文件系统  本地环境运行，需要使用此种方法获取文件系统
				URI uri = new URI("hdfs://wang:9000");//HDFS 地址
				fs = FileSystem.get(uri,conf);
				
				// 获取本地文件系统
				local = FileSystem.getLocal(conf);
				//只上传data/testdata 目录下 txt 格式的文件(进行文件过滤)
				FileStatus[] localStatus = local.globStatus(new Path("E://data/205/*"), new RegexAcceptPathFilter("^.*txt$"));
				
				// 获得所有文件路径
				Path[] listedPaths = FileUtil.stat2Paths(localStatus);
				Path dstPath = new Path("hdfs://wang:9000/mergehdfs/middle/");
		        for(Path p:listedPaths){
		        	//将本地文件上传到HDFS
		        	fs.copyFromLocalFile(p, dstPath);
		        }
	}

	private static class RegexAcceptPathFilter implements PathFilter {

private final String regex;
	
	public RegexAcceptPathFilter(String regex) {
		this.regex = regex;
	}
	
	@Override
	public boolean accept(Path path) {
		// TODO Auto-generated method stub
		boolean flag = path.toString().matches(regex);
		return flag;
	}
}
}