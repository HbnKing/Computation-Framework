package classic.cases;

import java.util.Map;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

/**
 * demo：
 * 实现数字累加的功能
 *
 */
public class StormTopologyAcker {
	
	public static class MySpout extends BaseRichSpout{
		
		private Map conf;//这里面保存的是storm的一些配置参数
		private TopologyContext context;//是storm的上下文对象
		private SpoutOutputCollector collector;// storm的发射器 负责向外面发射数据
		/**
		 * 这个方法只会执行一次，是一个初始化方法
		 * 后期如果也一些初始化链接的代码 需要放在这里面
		 */
		@Override
		public void open(Map conf, TopologyContext context,
				SpoutOutputCollector collector) {
			this.conf = conf;
			this.context = context;
			this.collector = collector;
		}
		
		
		int num = 0;
		/**
		 * 这个方法会被storm框架循环调用
		 * 
		 */
		@Override
		public void nextTuple() {
			num++;
			System.out.println("spout:"+num);
			
			//想要开启acker消息确认机制，第一步：首先在发送数据的时候给数据带一个messageId,这个messageId其实可以理解为这个tuple数据的唯一标识
			//或者理解为数据库中的主键id字段，要保证messageId和tuple中的数据是一一对应的，因为后期可能需要根据messageId来获取对应的处理失败的那个tuple的内容
			//并且messageId和tuple中数据的关系是需要程序员自己维护的
			this.collector.emit(new Values(num),num);//把num封装到一个tuple里面，发射出去
			Utils.sleep(1000);   //每隔一秒发一次
		}
		
		/**
		 * 声明输出字段
		 */
		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			declarer.declare(new Fields("num"));//注意：这个参数的值和tuple中的值是一一对应的
		}

		
		
		//第二部：需要实现ack和fail方法
		
		/**
		 * 数据处理成功会被调用
		 */
		@Override
		public void ack(Object msgId) {
			System.out.println("数据处理成功:"+msgId);
		}
		
		/**
		 * 数据处理失败会被调用
		 */
		@Override
		public void fail(Object msgId) {
			System.out.println("数据处理失败:"+msgId);
			//TODO 可以选择重发数据或者记录数据
		}
		
		
	}
	
	
	public static class MyBolt extends BaseRichBolt{
		
		private Map stormConf;
		private TopologyContext context;
		private OutputCollector collector;
		/**
		 * 初始化方法，只会执行一次
		 */
		@Override
		public void prepare(Map stormConf, TopologyContext context,
				OutputCollector collector) {
			this.stormConf = stormConf;
			this.context = context;
			this.collector = collector;
		}
		
		int sum = 0;
		/**
		 * 这个方法里面需要定义具体的业务逻辑代码
		 */
		@Override
		public void execute(Tuple input) {
			//第三步：需要在bolt里面确认tuple是否正确处理
			//注意：如果在这既没有调用ack也没有调用fail方法，那么当达到超时时间默认60s的时候，这个数据就会被认为处理失败，spout中的fail方法会被触发
			//input.getInteger(0);
			try{
				Integer num = input.getIntegerByField("num");
				System.out.println("线程id："+Thread.currentThread().getId()+"  num:"+num);
				this.collector.ack(input);//确认数据处理成功
			}catch(Exception e){
				this.collector.fail(input);//确认数据处理失败
			}
		}

		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			
		}
		
	}
	
	public static void main(String[] args) {
		TopologyBuilder topologyBuilder = new TopologyBuilder();
		topologyBuilder.setSpout("spout1", new MySpout());
		topologyBuilder.setBolt("bolt1", new MyBolt()).shuffleGrouping("spout1");
		
		StormTopology createTopology = topologyBuilder.createTopology();
		String topologyName = StormTopologyAcker.class.getSimpleName();
		Config config = new Config();
		if(args.length==0){
			//创建一个本地集群运行程序
			LocalCluster localCluster = new LocalCluster();
			localCluster.submitTopology(topologyName, config, createTopology);
		}else{
			try {
				StormSubmitter.submitTopology(topologyName, config, createTopology);
			} catch (AlreadyAliveException e) {
				e.printStackTrace();
			} catch (InvalidTopologyException e) {
				e.printStackTrace();
			} catch (AuthorizationException e) {
				e.printStackTrace();
			}
		}
	}
	
	

}
