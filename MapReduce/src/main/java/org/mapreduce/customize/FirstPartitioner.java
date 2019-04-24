package org.mapreduce.customize;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

/**
* 分区函数类。根据first确定Partition。
*/
public class FirstPartitioner extends Partitioner<IntPair, IntWritable>{
        @Override
        public int getPartition(IntPair key, IntWritable value,int numPartitions){
            return Math.abs(key.getFirst() * 127) % numPartitions;
        }
}