package org.hdfs;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.util.Progressable;

public class HdfsCommon  extends FileSystem{
	private  Configuration conf = new Configuration();
	private  FileSystem   fs = null ;
	
	
	public HdfsCommon(){
		try {
			fs = getFileSystem();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public HdfsCommon(URI uri ,String user){
		try {
			fs = getFileSystem(uri, user);
		} catch (IOException | InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public HdfsCommon(URI uri){
		try {
			fs = getFileSystem(uri);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	//获取文件系统  本地测试版
	private  FileSystem getFileSystem(URI uri) throws IOException  {
	    	
		//读取配置文件
		//Configuration conf = new Configuration();
		
		//返回默认文件系统  如果在 Hadoop集群下运行，使用此种方法可直接获取默认文件系统
		//FileSystem fs = FileSystem.get(conf);      
		
		//指定的文件系统地址
		//URI uri = new URI("hdfs://cloud004:9000");
		//返回指定的文件系统    如果在本地测试，需要使用此种方法获取文件系统
		FileSystem fs = FileSystem.get(uri,conf); 
		
		return fs;
	}
	//获取文件系统
	private FileSystem getFileSystem() throws IOException  {
		    	
		//读取配置文件
		//Configuration conf = new Configuration();
		//返回默认文件系统  如果在 Hadoop集群下运行，使用此种方法可直接获取默认文件系统
		FileSystem fs = FileSystem.get(conf);      
		//指定的文件系统地址
		//URI uri = new URI("hdfs://cloud004:9000");
		//返回指定的文件系统    如果在本地测试，需要使用此种方法获取文件系统
		//FileSystem fs = FileSystem.get(uri,conf); 
		
		return fs;
	}
	
	private FileSystem getFileSystem(URI uri ,String user) throws IOException, InterruptedException{
		FileSystem fs  = FileSystem.get(uri, conf, user);
		
		return fs ;
		
		
	}
	
	
	
	/**
	 * 创建文件夹
	 * @throws Exception
	 */
	public  void mkdir(String hdfsPath) throws Exception {
    	
		//获取文件系统
		//FileSystem fs = getFileSystem();
		
		//创建文件目录
		fs.mkdirs(new Path(hdfsPath));
		
		//释放资源
		fs.close();
	}
	//删除文件或者文件目录
	public  void rmdir(String hdfsPath) throws Exception {
	    	
		//返回FileSystem对象
		//FileSystem fs = getFileSystem();
		
		//删除文件或者文件目录  delete(Path f) 此方法已经弃用
		fs.delete(new Path(hdfsPath),true);
		
		//释放资源
		fs.close();
	}
	//获取目录下的所有文件
	public  void ListAllFile() throws IOException{
	    	
		//返回FileSystem对象
		FileSystem fs = getFileSystem();
		
		//列出目录内容
		FileStatus[] status = fs.listStatus(new Path("hdfs://cloud004:9000/middle/weibo/"));
		
		//获取目录下的所有文件路径
		Path[] listedPaths = FileUtil.stat2Paths(status);
		
		//循环读取每个文件
		for(Path p : listedPaths){
			
			System.out.println(p);
			
		}
		//释放资源
		fs.close();
	}
	//文件上传至 HDFS
	public  void copyToHDFS() throws IOException{
	    	
		//返回FileSystem对象
		FileSystem fs = getFileSystem();
		
		//源文件路径是Linux下的路径，如果在 windows 下测试，需要改写为Windows下的路径，比如D://hadoop/djt/weibo.txt
		Path srcPath = new Path("/home/hadoop/djt/weibo.txt");
		
		// 目的路径
		Path dstPath = new Path("hdfs://cloud004:9000/middle/weibo");
		
		//实现文件上传
		fs.copyFromLocalFile(srcPath, dstPath);
		
		//释放资源
		fs.close();
	}
	//获取 HDFS 集群节点信息
	public  void getHDFSNodes() throws IOException{
	    	
		//返回FileSystem对象
		FileSystem fs = getFileSystem();
		
		//获取分布式文件系统
		DistributedFileSystem hdfs = (DistributedFileSystem)fs;
		
		//获取所有节点
		DatanodeInfo[] dataNodeStats = hdfs.getDataNodeStats();
		//循环打印所有节点
		for(int i=0;i< dataNodeStats.length;i++){
			System.out.println("DataNode_"+i+"_Name:"+dataNodeStats[i].getHostName());
		}
	}
	
	//查找某个文件在 HDFS 集群的位置
	public  void getFileLocal() throws IOException{
	    	
		//返回FileSystem对象
		FileSystem fs = getFileSystem();
		
		//文件路径
		Path path = new Path("hdfs://cloud004:9000/middle/weibo/weibo.txt");
		
		//获取文件目录
		FileStatus filestatus = fs.getFileStatus(path);
		//获取文件块位置列表
		BlockLocation[] blkLocations = fs.getFileBlockLocations(filestatus, 0, filestatus.getLen());
		//循环输出块信息
		for(int i=0;i< blkLocations.length;i++){
			String[] hosts = blkLocations[i].getHosts();
			System.out.println("block_"+i+"_location:"+hosts[0]);
		}
	}

	@Override
	public URI getUri() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public FSDataInputStream open(Path f, int bufferSize) throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public FSDataOutputStream create(Path f, FsPermission permission, boolean overwrite, int bufferSize,
			short replication, long blockSize, Progressable progress) throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public FSDataOutputStream append(Path f, int bufferSize, Progressable progress) throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean rename(Path src, Path dst) throws IOException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean delete(Path f, boolean recursive) throws IOException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public FileStatus[] listStatus(Path f) throws FileNotFoundException, IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void setWorkingDirectory(Path new_dir) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public Path getWorkingDirectory() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean mkdirs(Path f, FsPermission permission) throws IOException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public FileStatus getFileStatus(Path f) throws IOException {
		// TODO Auto-generated method stub
		return null;
	}
	
	

}
