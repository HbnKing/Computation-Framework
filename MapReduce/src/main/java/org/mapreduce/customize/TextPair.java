package org.mapreduce.customize;


//通过自定义Writable 类 
import java.io.*;
import org.apache.hadoop.io.*;

//writable comparable   
//实现了writable 接口 的 类 可以被   hadoop 读写
// 求序列化要快，且体积要小，占用带宽要小。所以必须理解Hadoop的序列化机制。

public class TextPair implements WritableComparable<TextPair>{
	private	Text first;//Text 类型的实例变量 first
	private	Text second;//Text 类型的实例变量 second
	
	public TextPair() {
		set(new Text(),new Text());
	}
	
	public TextPair(String first, String second) {
		set(new Text(first),new Text(second));
	}
	
	public TextPair(Text first, Text second) {
		set(first, second);
	}
	
	public void set(Text first, Text second) {
		this.first = first;
		this.second = second;
	}
	
	public Text getFirst() {
		return first;
	}
	
	public Text getSecond() {
		return second;
	}
	
	//将对象转换为字节流并写入到输出流out中
	@Override
	public void write(DataOutput out)throws IOException {
		first.write(out);
		second.write(out);
	}
	
	//从输入流in中读取字节流反序列化为对象
	@Override
	public void readFields(DataInput in)throws IOException {
		first.readFields(in);
		second.readFields(in);
	}
	
	@Override
	public int hashCode() {
		return first.hashCode() *163+ second.hashCode();
	}
	
	
	@Override
	public boolean equals(Object o) {
		if(o instanceof TextPair) {
			TextPair tp = (TextPair) o;
			return first.equals(tp.first) && second.equals(tp.second);
		}
			return false;
	}
	
	@Override
	public String toString() {
		return first +"\t"+ second;
	}
	
	//排序
	@Override
	public int compareTo(TextPair tp) {
		int cmp = first.compareTo(tp.first); 
		if(cmp !=0) {
			return cmp;
		}
		return second.compareTo(tp.second);
	}

}