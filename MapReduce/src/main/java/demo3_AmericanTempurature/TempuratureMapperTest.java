package com.work.hadoopwork;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Before;
import org.junit.Test;
/**
 * Mapper 端的单元测试
 */
@SuppressWarnings("all")
public class TempuratureMapperTest {
	private Mapper mapper;//定义一个Mapper对象
	private MapDriver driver;//定义一个MapDriver 对象

	@Before
	public void init() {
		mapper = new AmericanTemprature.TemperatureMapper();//实例化一个Temperature中的TemperatureMapper对象
		driver = new MapDriver(mapper);//实例化MapDriver对象
	}

	@Test
	public void test() throws IOException {
		//输入一行测试数据
		String line = "1997 06 30 16   200   -61 10127   190    72     0     0 -9999";
		driver.withInput(new LongWritable(), new Text(line))//跟TemperatureMapper输入类型一致
				.withOutput(new Text("03103"), new IntWritable(200))//跟TemperatureMapper输出类型一致
				.runTest();
	}
}