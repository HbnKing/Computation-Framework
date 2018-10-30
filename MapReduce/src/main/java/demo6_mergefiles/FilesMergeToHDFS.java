package com.hadoop.mergefiles;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.IOUtils;




public class FilesMergeToHDFS {
	private static FileSystem fs = null;
	private static FileSystem local = null;

	public static void main(String[] args) throws IOException, URISyntaxException {
		// TODO Auto-generated method stub
		list();
	}
		
	
	
		
	
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
				//只上传data/73 目录下 txt 格式的文件(进行文件过滤,去掉svn)
				FileStatus[] dirsStatus = local.globStatus(new Path("E://data/73/*"), new RegexExcludePathFilter("^.*svn$"));
				
				// 获得所有文件路径
				Path[] dirs = FileUtil.stat2Paths(dirsStatus);
				
				FSDataInputStream in = null;
	        	FSDataOutputStream out = null;
				for(Path p:dirs){
					
		        	//System.out.println(p);
		        	//将本地文件上传到HDFS

					String filename = p.getName();
					FileStatus[] localStatus = local.globStatus(new Path(p+"/*"),new RegexAcceptPathFilter("^.*txt$"));
		        	Path[] listedPaths = FileUtil.stat2Paths(localStatus);
		        	
		        	//设置输出路径
		        	Path block = new Path("hdfs://wang:9000/mergehdfs/filesmerge/"+filename+".txt");
		        	//设置输出流
		        	out =fs.create(block);
		        	for(Path path:listedPaths){
		        		//System.out.println(path);
		        		
		        		in = local.open(path);
		        		//一个输入，一个输出，第三个是缓存大小，第四个指定拷贝完毕后是否关闭流
		        		IOUtils.copyBytes(in, out, 4096, false); // 复制数据
		        		in.close();
		        	}
		        	if (out != null) {
						// 关闭输出流
						out.close();
					}
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
private static class RegexExcludePathFilter  implements PathFilter {

private final String regex;
	
	public RegexExcludePathFilter (String regex) {
		this.regex = regex;
	}
	
	@Override
	public boolean accept(Path path) {
		// TODO Auto-generated method stub
		boolean flag = path.toString().matches(regex);
		return !flag;
	}
}
}