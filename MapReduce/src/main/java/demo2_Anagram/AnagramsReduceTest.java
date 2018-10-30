package com.work.hadoopwordcount;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

public class AnagramsReduceTest {
	private Reducer reducer;
	private ReduceDriver driver;
	
	
	@Before
	public void init() {
		reducer = new Anagrams.Anagramsreducer();
		driver = new ReduceDriver(reducer);
		
	}
	
	@Test
	public void test() throws IOException {
		
		Text key = new Text("abcdefg"); //�½�һ��Key������̶�����
		
		List values = new ArrayList();  //���½������б���д��4����ĸ��Valueֵ��Ŀ����֤������Ƿ�Ԥ����ʽ���
		values.add(new Text("gfedcba"));
		values.add(new Text("decgfba"));
		values.add(new Text("fedgcba"));
		values.add(new Text("gcbfeda"));
		
		driver.withInput(key, values)
		      .withOutput(key, new Text("gfedcba,decgfba,fedgcba,gcbfeda"))  //��֤�Ƿ񰴴˸�ʽ���
		      .runTest();
		
	}
	

}
