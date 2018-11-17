package assignment.secondarySorting;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

public class CompositeKeyWritable implements Writable, WritableComparable<CompositeKeyWritable> {
	String ipAddress;
	Date ipAccessDate;
//	SimpleDateFormat format=new SimpleDateFormat("dd/MMM/yyyy");
	CompositeKeyWritable(){
		
	}
	CompositeKeyWritable(String ipAddress,Date ipAccessDate){
		this.ipAddress=ipAddress;
		this.ipAccessDate=ipAccessDate;
	}
	public String getIpAddress() {
		return ipAddress;
	}

	public void setIpAddress(String ipAddress) {
		this.ipAddress = ipAddress;
	}

	public Date getIpAccessDate() {
		return ipAccessDate;
	}

	public void setIpAccessDate(Date ipAccessDate) {
		this.ipAccessDate = ipAccessDate;
	}

	public void write(DataOutput out) throws IOException {
		WritableUtils.writeString(out, ipAddress);
		WritableUtils.writeString(out, String.valueOf(ipAccessDate));
		
	}

	public void readFields(DataInput in) throws IOException {
		ipAddress=WritableUtils.readString(in);
		
			ipAccessDate=new Date(WritableUtils.readString(in));
		
	}
	
	public String toString() {
		return ipAddress+"\t"+ipAccessDate;
	}
	

	public int compareTo(CompositeKeyWritable objectPair) {
		int result=ipAddress.compareTo(objectPair.getIpAddress());
		if(result==0) {
			result=ipAccessDate.compareTo(objectPair.getIpAccessDate());
		}
		return result;
		
	}
	

}
