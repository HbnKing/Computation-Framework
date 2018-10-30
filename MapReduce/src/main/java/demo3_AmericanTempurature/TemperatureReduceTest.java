package com.work.hadoopwork;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;
/**
 * Reducer 单元测试
 */
@SuppressWarnings("all")
public class TemperatureReduceTest {
	private Reducer reducer;//定义一个Reducer对象	
	private ReduceDriver driver;//定义一个ReduceDriver对象

	@Before
	public void init() {
		reducer = new AmericanTemprature.TemperatureReducer();//实例化一个Temperature中的TemperatureReducer对象
		driver = new ReduceDriver(reducer);//实例化ReduceDriver对象
	}

	@Test
	public void test() throws IOException {
		//String key = "weatherStationId";//声明一个key值
		String key = "03103";
		List values = new ArrayList();
		values.add(new IntWritable(200));//添加第一个value值
		values.add(new IntWritable(100));//添加第二个value值
		driver.withInput(new Text(key), values)//跟TemperatureReducer输入类型一致
			  .withOutput(new Text(key), new IntWritable(150))//跟TemperatureReducer输出类型一致
			  .runTest();
	}
}