package assignment.customWritablestock;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Date;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

public class StockAnalysisWritable implements Writable {
	String maxStockVolumeDate;
	String minStockVolumeDate;
	float maxAdjClosingPrice;
	long minStockVolume;
	long maxStockVolume;
	
	StockAnalysisWritable(){
		
	}
	
	public void readFields(DataInput in) throws IOException {
		maxStockVolumeDate=WritableUtils.readString(in);
		minStockVolumeDate=WritableUtils.readString(in);
		maxAdjClosingPrice=in.readFloat();
		minStockVolume=in.readLong();
		maxStockVolume=in.readLong();
	}
	
	
	StockAnalysisWritable(String maxStockVolumeDate,String minStockVolumeDate,float maxAdjClosingPrice,long minStockVolume,long maxStockVolume){
		this.maxStockVolumeDate=maxStockVolumeDate;
		this.minStockVolumeDate=minStockVolumeDate;
		this.maxAdjClosingPrice=maxAdjClosingPrice;
		this.minStockVolume=minStockVolume;
		this.maxStockVolume=maxStockVolume;
	}	
	
	
	public void write(DataOutput out) throws IOException {
		WritableUtils.writeString(out,String.valueOf(maxStockVolumeDate));
		WritableUtils.writeString(out,String.valueOf(minStockVolumeDate));
		out.writeFloat(maxAdjClosingPrice);
		out.writeLong(minStockVolume);
		out.writeLong(maxStockVolume);
		
	}
	public String getMaxStockVolumeDate() {
		return maxStockVolumeDate;
	}

	public void setMaxStockVolumeDate(String maxStockVolumeDate) {
		this.maxStockVolumeDate = maxStockVolumeDate;
	}

	public String getMinStockVolumeDate() {
		return minStockVolumeDate;
	}

	public void setMinStockVolumeDate(String minStockVolumeDate) {
		this.minStockVolumeDate = minStockVolumeDate;
	}

	public float getMaxAdjClosingPrice() {
		return maxAdjClosingPrice;
	}

	public void setMaxAdjClosingPrice(float maxAdjClosingPrice) {
		this.maxAdjClosingPrice = maxAdjClosingPrice;
	}
	public long getMinStockVolume() {
		return minStockVolume;
	}
	public void setMinStockVolume(long minStockVolume) {
		this.minStockVolume=minStockVolume;
	}
	
	public long getMaxStockVolume() {
		return maxStockVolume;
	}
	public void setMaxStockVolume(long maxStockVolume) {
		this.maxStockVolume=maxStockVolume;
	}

	public String toString() {
		return "Maximum Stock Value Date "+maxStockVolumeDate+"\tMinimum Stock Value Date "+minStockVolumeDate+"\tMaximum Adj Closing Price"+maxAdjClosingPrice;
	}

	
	

}
