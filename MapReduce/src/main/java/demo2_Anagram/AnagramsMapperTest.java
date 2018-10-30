package com.work.hadoopwordcount;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Before;
import org.junit.Test;


public class AnagramsMapperTest {
	private Mapper mapper;
	private MapDriver driver;
	
	@Before
	public void init() {
		mapper = new Anagrams.Anagramsmapper();
		driver = new MapDriver(mapper);
	}
	
	@Test
	public void test() throws IOException {
		String line = "gfedcba";  //�Զ������ĸ����֤�������Ƿ����ȷ����
		driver.withInput(new LongWritable(), new Text(line))
		      .withOutput(new Text("abcdefg"),new Text("gfedcba"))  //��֤���Key�Ƿ��������ĸ�������Value����
		      .runTest();
		
	}

}
