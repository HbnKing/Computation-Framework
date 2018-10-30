package teacher;
//小文件合并作业


import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.IOUtils;
/**
 * function 合并小文件至 HDFS 
 * @author 小讲
 *
 */
public class MergeSmallFilesToHDFS {
	private static FileSystem fs = null;
	private static FileSystem local = null;
	/**
	 * @function main 
	 * @param args
	 * @throws IOException
	 * @throws URISyntaxException
	 */
	public static void main(String[] args) throws IOException,
			URISyntaxException {
		list();
	}

	/**
	 * 
	 * @throws IOException
	 * @throws URISyntaxException
	 */
	public static void list() throws IOException, URISyntaxException {
		// 读取hadoop文件系统的配置
		Configuration conf = new Configuration();
		//文件系统访问接口
		URI uri = new URI("hdfs://dajiangtai:9000");
		//创建FileSystem对象
		fs = FileSystem.get(uri, conf);
		// 获得本地文件系统
		local = FileSystem.getLocal(conf);
		//过滤目录下的 svn 文件，globStatus从第一个参数通配符合到文件，剔除满足第二个参数到结果，因为PathFilter中accept是return!  
		FileStatus[] dirstatus = local.globStatus(new Path("D://data/73/*"),new RegexExcludePathFilter("^.*svn$"));
		//获取73目录下的所有文件路径，注意FIleUtil中stat2Paths()的使用，它将一个FileStatus对象数组转换为Path对象数组。
		Path[] dirs = FileUtil.stat2Paths(dirstatus);
		FSDataOutputStream out = null;
		FSDataInputStream in = null;
		for (Path dir : dirs) {
			String fileName = dir.getName().replace("-", "");//文件名称
			//只接受日期目录下的.txt文件，^匹配输入字符串的开始位置,$匹配输入字符串的结束位置,*匹配0个或多个字符。
			FileStatus[] localStatus = local.globStatus(new Path(dir+"/*"),new RegexAcceptPathFilter("^.*txt$"));
			// 获得日期目录下的所有文件
			Path[] listedPaths = FileUtil.stat2Paths(localStatus);
			//输出路径
			Path block = new Path("hdfs://dajiangtai:9000/middle/tv/"+ fileName + ".txt");
			// 打开输出流
			out = fs.create(block);			
			for (Path p : listedPaths) {
				in = local.open(p);// 打开输入流
				//一个输入，一个输出，第三个是缓存大小，第四个指定拷贝完毕后是否关闭流
				IOUtils.copyBytes(in, out, 4096, false); // 复制数据，IOUtils.copyBytes可以方便地将数据写入到文件，不需要自己去控制缓冲区，也不用自己去循环读取输入源。false表示不自动关闭数据流，那么就手动关闭。
				// 关闭输入流
				in.close();
			}
			if (out != null) {
				// 关闭输出流
				out.close();
			}
		}
		
	}

	/**
	 * 
	 * @function 过滤 regex 格式的文件
	 *
	 */
	public static class RegexExcludePathFilter implements PathFilter {
		private final String regex;

		public RegexExcludePathFilter(String regex) {
			this.regex = regex;
		}

		@Override
		public boolean accept(Path path) {
			// TODO Auto-generated method stub
			boolean flag = path.toString().matches(regex);
			return !flag;
		}

	}

	/**
	 * 
	 * @function 接受 regex 格式的文件
	 *
	 */
	public static class RegexAcceptPathFilter implements PathFilter {
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
