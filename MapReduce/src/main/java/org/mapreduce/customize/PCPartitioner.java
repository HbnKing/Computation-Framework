package org.mapreduce.customize;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class PCPartitioner extends Partitioner< Text, Text> {
	@Override
	public int getPartition(Text key, Text value, int numReduceTasks) {
		// TODO Auto-generated method stub
		String[] nameAgeScore = value.toString().split("\t");
		String age = nameAgeScore[1];//学生年龄
		int ageInt = Integer.parseInt(age);//按年龄段分区

		// 默认指定分区 0
		if (numReduceTasks == 0)
			return 0;
		
		//年龄小于等于20，指定分区0
		if (ageInt <= 20) {
			return 0;
		}
		// 年龄大于20，小于等于50，指定分区1
		if (ageInt > 20 && ageInt <= 50) {

			return 1 % numReduceTasks;
		}
		// 剩余年龄，指定分区2
		else
			return 2 % numReduceTasks;
	}
}