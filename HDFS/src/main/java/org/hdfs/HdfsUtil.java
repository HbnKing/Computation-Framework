package org.hdfs;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;  
import java.util.Properties;

import org.apache.hadoop.io.IOUtils;  
import org.apache.hadoop.conf.Configuration;  
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;  
import org.apache.hadoop.fs.FileSystem;  
import org.apache.hadoop.fs.LocatedFileStatus;  
import org.apache.hadoop.fs.Path;  
import org.apache.hadoop.fs.RemoteIterator;

  /**
   * 2018年1月
   * @author Wangheng
   *
   */
public class HdfsUtil {  
	
	private static Properties prop = new Properties(); 
	private static FileSystem fs=null;  
	private static String hdfsUrl = null;
	private static String hdfsUser = null;
      
    /**
     * 初始化获取 文件系统  fs
     * @throws Exception
     */
    private static void init() throws Exception{  
    	
        prop.load(new FileInputStream("props/hdfs.properties")); 
        hdfsUrl = prop.getProperty("hdfsUrl","").trim(); 
        hdfsUser = prop.getProperty("hdfsUser","hdfs").trim();
        
        /* 
         *  
         * 注意注意注意 
         * windows上eclipse上运行，hadoop的bin文件下windows环境编译的文件和hadoop版本关系紧密 
         * 如果报错，或者api无效，很可能是bin下hadoop文件问题 
         *  
         */  
          
        //读取classpath下的core/hdfs-site.xml 配置文件，并解析其内容，封装到conf对象中  
        //如果没有会读取hadoop里的配置文件  
        Configuration conf=new Configuration();  
          
        //也可以在代码中对conf中的配置信息进行手动设置，会覆盖掉配置文件中的读取的值  
        //设置文件系统为hdfs。如果设置这个参数，那么下面的方法里的路径就不用写hdfs://hello110:9000/  
        //conf.set("fs.defaultFS", "hdfs://hello110:9000/");  
          
        //根据配置信息，去获取一个具体文件系统的客户端操作实例对象  
        fs=FileSystem.get(new URI(hdfsUrl), conf ,hdfsUser);  
          
/*        Iterator<Entry<String, String>> iterator = conf.iterator();  
        while(iterator.hasNext()){  
            Entry<String, String> ent=iterator.next();  
            System.out.println(ent.getKey()+"  :  "+ent.getValue());  
        }  
*/          
       // System.out.println("-----------------/hdfs-site.xml 文件读取结束----------------");  
    }  
      
    public static void listFiles() throws Exception{  
          init();
        //RemoteIterator 远程迭代器  
        RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path(hdfsUrl), true);  
        
        while(listFiles.hasNext()){  
            LocatedFileStatus file = listFiles.next();  
            Path path = file.getPath();  
            //String fileName=path.getName();  
            System.out.println(path.toString());  
            System.out.println("权限："+file.getPermission());  
            System.out.println("组："+file.getGroup());  
            System.out.println("文件大小："+file.getBlockSize());  
            System.out.println("所属者："+file.getOwner());  
            System.out.println("副本数："+file.getReplication());  
              
            BlockLocation[] blockLocations = file.getBlockLocations();  
            for(BlockLocation bl:blockLocations){  
                System.out.println("块起始位置："+bl.getOffset());  
                System.out.println("块长度："+bl.getLength());  
                String[] hosts = bl.getHosts();  
                for(String h:hosts){  
                    System.out.println("块所在DataNode："+h);  
                }  
                  
            }  
              
            System.out.println("*****************************************");  
            
              
        }  
          
/*        System.out.println("-----------------华丽的分割线----------------");  
          
        FileStatus[] listStatus = fs.listStatus(new Path(hdfs));  
          
        for(FileStatus status:listStatus){  
            String name=status.getPath().getName();  
            System.out.println(name+(status.isDirectory()?" is Dir":" is File"));  
        }  */
    }  
      
    /**
     * 
     * @param hdfsPath
     * @param localPath
     * @throws Exception
     */
    public static void upload(String hdfsPath,String localPath) throws Exception{  
        init();
        System.out.println("-----------------upload----------------");  
          
        Path path = new Path(hdfsPath);  
        FSDataOutputStream os = fs.create(path);  
          
        FileInputStream is = new FileInputStream(localPath);  
          
        IOUtils.copyBytes(is, os, 1024, true);  
    }  
      
    /**
     * 
     * @throws Exception
     */
    public static void upload2() throws Exception{  
        init(); 
        System.out.println("-----------------upload2----------------");  
        //hdfs dfs -copyFromLocal 从本地拷贝命令  
        fs.copyFromLocalFile(new Path("d:/testFS-2.txt"), new Path("/testdata/testFs6.txt"));  
        //如果windows-hadoop环境不行，可以用下面的api，最后的true是：用java的io去写。  
        //fs.copyToLocalFile(false, new Path("d:/testFS-2.txt"), new Path("/testdata/testFs6.txt"), true);  
        fs.close();  
          
    }  
    public  static void download(String hdfsPath,String localPath) throws Exception {
    	init();
    	 FSDataInputStream inGet = fs.open(new Path(hdfsPath));
         FileOutputStream outGet = new FileOutputStream(localPath);
         IOUtils.copyBytes(inGet, outGet, 1024, true);

    	
    }
    
    
    
    
      /**
       * 
       * @param dirName
       * @throws Exception
       */
    public static void mkdir(String dirName) throws Exception{  
        init();  
        System.out.println("-----------------mkdir----------------");  
          
        fs.mkdirs(new Path(dirName));  
    }  
      /**
       * 
       * @param fileName
       * @throws Exception
       */
    public  static void rm(String fileName) throws Exception{  
    	init();
        //true：如果是目录也会删除。false，遇到目录则报错  
        fs.delete(new Path(hdfsUrl+fileName), true);  
    }  
    
    
    public static String getStringByTXT(String txtFilePath) throws Exception{ 
    	
        StringBuffer buffer = new StringBuffer();  
        FSDataInputStream fsr = null;  
        BufferedReader bufferedReader = null;  
        String lineTxt = null;  
        init();
        fsr = fs.open(new Path(txtFilePath));  
        bufferedReader = new BufferedReader(new InputStreamReader(fsr));          
        while ((lineTxt = bufferedReader.readLine()) != null){  
        	if(lineTxt.split("\t")[0].trim().equals("00067")){  
             return lineTxt;  
        	}  
        	buffer.append(lineTxt);
        }  
        	release(bufferedReader);
  
        return buffer.toString();  
    }  
    private static void  release(BufferedReader bufferedReader ) throws IOException {
    	if (bufferedReader != null){  
           bufferedReader.close();  
        }  
    }
    
	  /**
	   * 
	   * @param txtFilePath
	   * @return
	 * @throws Exception 
	   */
    public static BufferedReader getBufferedReader(String txtFilePath) throws Exception{  
    	init();

        FSDataInputStream fsr = null;  
        BufferedReader bufferedReader = null;  
        fsr = fs.open(new Path(txtFilePath));  
        bufferedReader = new BufferedReader(new InputStreamReader(fsr));          
  
        return bufferedReader;  
    } 
      
}  
