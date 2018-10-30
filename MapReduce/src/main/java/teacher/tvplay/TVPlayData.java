package teacher.tvplay;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
/**
 * 
 * @author yangjun
 * @function 自定义对象
 */
public class TVPlayData implements WritableComparable<Object>{
	private int daynumber;
	private int collectnumber;
	private int commentnumber;
	private int againstnumber;
	private int supportnumber;
	public TVPlayData(){}
	public void set(int daynumber,int collectnumber,int commentnumber,int againstnumber,int supportnumber){
		this.daynumber = daynumber;
		this.collectnumber = collectnumber;
		this.commentnumber = commentnumber;
		this.againstnumber = againstnumber;
		this.supportnumber = supportnumber;
	}
	public int getDaynumber() {
		return daynumber;
	}
	public void setDaynumber(int daynumber) {
		this.daynumber = daynumber;
	}
	public int getCollectnumber() {
		return collectnumber;
	}
	public void setCollectnumber(int collectnumber) {
		this.collectnumber = collectnumber;
	}
	public int getCommentnumber() {
		return commentnumber;
	}
	public void setCommentnumber(int commentnumber) {
		this.commentnumber = commentnumber;
	}
	public int getAgainstnumber() {
		return againstnumber;
	}
	public void setAgainstnumber(int againstnumber) {
		this.againstnumber = againstnumber;
	}
	public int getSupportnumber() {
		return supportnumber;
	}
	public void setSupportnumber(int supportnumber) {
		this.supportnumber = supportnumber;
	}
	@Override
	public void readFields(DataInput in) throws IOException {
		daynumber = in.readInt();
		collectnumber = in.readInt();
		commentnumber = in.readInt();
		againstnumber = in.readInt();
		supportnumber = in.readInt();
	}
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(daynumber);
		out.writeInt(collectnumber);
		out.writeInt(commentnumber);
		out.writeInt(againstnumber);
		out.writeInt(supportnumber);
	}
	@Override
	public int compareTo(Object o) {
		return 0;
	};
}