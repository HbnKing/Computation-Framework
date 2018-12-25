package zkDemo.test.java;


import java.util.List;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

/**
 * 监视器
 * 监视zk指定节点的情况(监控zk中指定节点的临时子节点的变化情况)
 * 
 *
 */
public class ZkNodeWatcher implements Watcher {
	
	private CuratorFramework client;
	private List<String> childList;
	public ZkNodeWatcher() {
		RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
		String zookeeperConnectionString = "sla1:2181";
		int sessionTimeoutMs = 5000;//会话超时时间，这个值只能在4000~40000之间 ，表示会话断掉之后，多长时间临时节点会被删除
		int connectionTimeoutMs = 3000;//获取zk链接的超时时间
		client = CuratorFrameworkFactory.newClient(zookeeperConnectionString, sessionTimeoutMs, connectionTimeoutMs, retryPolicy);
		client.start();//开始客户端
		
		try {
			//注意：监视器注册一次只能使用一次，多次使用需要重复注册
			childList = client.getChildren().usingWatcher(this).forPath("/monitor");
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}
	
	/**
	 * 当监控的节点发生了变化，这个方法会被调用
	 */
	@Override
	public void process(WatchedEvent event) {
		//System.out.println("执行了。。。"+event);
		try {
			//获取节点下面最新的所有临时子节点
			List<String> newChildList = client.getChildren().usingWatcher(this).forPath("/monitor");
			for (String ip : childList) {
				if(!newChildList.contains(ip)){
					System.out.println("节点消失："+ip);
					//TODO 发邮件javamail 发短信  打电话
				}
			}
			
			for (String ip : newChildList) {
				if(!childList.contains(ip)){
					System.out.println("新增节点："+ip);
				}
			}
			
			//注意
			this.childList = newChildList;
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public static void main(String[] args) {
		ZkNodeWatcher zkNodeWatcher = new ZkNodeWatcher();
		zkNodeWatcher.start();
	}
	//创建死循环
	private void start() {
		while(true){
			;
		}
	}
	
	

}
