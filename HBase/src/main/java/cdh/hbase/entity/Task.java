package cdh.hbase.entity;

public class Task {
	private String rowkey;//id+时间
	private String systaskid;//系统任务id
	private String taskname;//任务名称
	private String type ;//任务类别：作业、测量
	private String state;//课程状态：已完成、接受
	public String getRowkey() {
		return rowkey;
	}
	public void setRowkey(String rowkey) {
		this.rowkey = rowkey;
	}
	public String getSystaskid() {
		return systaskid;
	}
	public void setSystaskid(String systaskid) {
		this.systaskid = systaskid;
	}
	public String getTaskname() {
		return taskname;
	}
	public void setTaskname(String taskname) {
		this.taskname = taskname;
	}
	public String getType() {
		return type;
	}
	public void setType(String type) {
		this.type = type;
	}
	public String getState() {
		return state;
	}
	public void setState(String state) {
		this.state = state;
	}
	

	
}
