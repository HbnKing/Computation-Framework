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
public class StormTopologyFieldsGrouping {
	
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
			this.collector.emit(new Values(num,num%2));//tuple 一次发送多个数据//把num封装到一个tuple里面，发射出去
			Utils.sleep(1000);
		}
		
		/**
		 * 声明输出字段
		 */
		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			declarer.declare(new Fields("num","flag"));//注意：这个参数的值和tuple中的值是一一对应的
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
			//input.getInteger(0);
			Integer num = input.getIntegerByField("num");
			System.err.println("线程id："+Thread.currentThread().getId()+"  num:"+num);
		}

		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			
		}
		
	}
	
	public static void main(String[] args) {
		TopologyBuilder topologyBuilder = new TopologyBuilder();
		topologyBuilder.setSpout("spout1", new MySpout());
		//特殊情况： 两个分类的数据，如果有1个线程或者3个线程去处理数据，会发生什么现象呢？(自己手工试验一下)
		topologyBuilder.setBolt("bolt1", new MyBolt(),3).fieldsGrouping("spout1", new Fields("flag"));
		
		
		StormTopology createTopology = topologyBuilder.createTopology();
		String topologyName = StormTopologyFieldsGrouping.class.getSimpleName();
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
