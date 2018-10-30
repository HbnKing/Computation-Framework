package com.hadoop.mapreduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

/*tvplaydata   类  是定义数据格式,将后面五个数据值定义为一个共同的数据类型(包含).
 * 5个网站的 每天电视剧的 播放量 收藏数 评论数 踩数 赞数
数据格式  电视剧名  电视剧类型   播放数   收藏数 评论数  踩数  赞数
1-5数字和5大视频的关系：1优酷2搜狐3土豆4爱奇艺5迅雷看看
继承者们	1	4105447	202	844	48	671
继承者们精华版	4	1996	0	0	0	0
光与影	2	1172	0	0	0	4
光荣使命	2	20359	0	0	0	0*/
public  class TVplaydata implements WritableComparable<Object>{
	//private String tvname;
	
	private int tvplaynum;
	private int tvfavorite;
	private int tvcomment;
	private int tvdown;
	private int tvvote;
public TVplaydata(){}
public void set(int tvplaynum,int tvfavorite,int tvcomment,int tvdown,int tvvote){
	this.tvplaynum = tvplaynum;
	this.tvfavorite = tvfavorite;
	this.tvcomment = tvcomment;
	this.tvdown = tvdown;
	this.tvvote = tvvote;
}
//source  get  set  
public void setTvpalynum(int tvplaynum) {
	this.tvplaynum = tvplaynum;
}
public int getTvpalynum() {
	return tvplaynum;
	
}

public int getTvfavorite() {
	return tvfavorite;
}
public void setTvfavorite(int tvfavorite) {
	this.tvfavorite = tvfavorite;
}
public int getTvcomment() {
	return tvcomment;
}
public void setTvcomment(int tvcomment) {
	this.tvcomment = tvcomment;
}
public int getTvdown() {
	return tvdown;
}
public void setTvdown(int tvdown) {
	this.tvdown = tvdown;
}
public int getTvvote() {
	return tvvote;
}
public void setTvvote(int tvvote) {
	this.tvvote = tvvote;
}
	@Override
	//反序列化
	//实现writablecomparable  类的 readfileds 方法   将输入流转为对象
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		tvplaynum = in.readInt();
		tvfavorite = in.readInt();
		tvcomment = in.readInt();
		tvdown = in.readInt();
		tvvote = in.readInt();
	}

	@Override
	//序列化过程  实现writablecomparable  类的 write 方法    将对象转为值
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		out.writeInt(tvplaynum);
		out.writeInt(tvfavorite);
		out.writeInt(tvcomment);
		out.writeInt(tvdown);
		out.writeInt(tvvote);
	}

	@Override
	public int compareTo(Object o) {
		// TODO Auto-generated method stub
		return 0;
	}
	
}
