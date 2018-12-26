package wh0906;

import java.net.InetAddress;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs.Ids;
import org.junit.Test;


public class TestCurator {
	@Test
	public  void  test1() throws Exception{
		//1创建客户端链接
		RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
				String zookeeperConnectionString = "sla1:2181";
				int sessionTimeoutMs = 5000;//会话超时时间，这个值只能在4000~40000之间 ，表示会话断掉之后，多长时间临时节点会被删除
				int connectionTimeoutMs = 3000;//获取zk链接的超时时间
				CuratorFramework client = CuratorFrameworkFactory.newClient(zookeeperConnectionString, sessionTimeoutMs, connectionTimeoutMs, retryPolicy);
				
				//CuratorFramework client = CuratorFrameworkFactory.newClient(zookeeperConnectionString , retryPolicy);
				client.start();
				
				/*Calling ZooKeeper Directly
				client.create().forPath("/my/path", myData)*/
				
				String hostname  =  InetAddress.getLocalHost().getHostName();
				String hostip = InetAddress.getLocalHost().getHostAddress();
				//创建节点     指定节点信息
				client.create().creatingParentsIfNeeded()
					.withMode(CreateMode.EPHEMERAL)//(CreateMode.PERSISTENT)   //指定节点类型
					.withACL(Ids.OPEN_ACL_UNSAFE)		//指定节点权限信息
					.forPath("/monitor/" +"_"+hostip);	//指定节点名称
		
				
				while(true ){
					
				;
				}
				
				
	}

}
