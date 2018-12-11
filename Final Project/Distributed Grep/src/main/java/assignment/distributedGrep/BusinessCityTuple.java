package assignment.distributedGrep;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

public class BusinessCityTuple implements Writable {
	String businessName;
	String businessCity;
	String category;
	
	public String getCategory() {
		return category;
	}


	public void setCategory(String category) {
		this.category = category;
	}


	public String getBusinessName() {
		return businessName;
	}


	public void setBusinessName(String businessName) {
		this.businessName = businessName;
	}


	public String getBusinessCity() {
		return businessCity;
	}


	public void setBusinessCity(String businessCity) {
		this.businessCity = businessCity;
	}


	public String toString() {
		return "BUSINESS Name: "+businessName+"\t"+"BUSINESS CITY: "+businessCity+"\t"+"BUSINESS CATEGORY "+category;
	}


	public void write(DataOutput out) throws IOException {
		WritableUtils.writeString(out, businessName);
		WritableUtils.writeString(out, businessCity);
		WritableUtils.writeString(out, category);
		
	}


	public void readFields(DataInput in) throws IOException {
		businessName=WritableUtils.readString(in);
		businessCity=WritableUtils.readString(in);
		category=WritableUtils.readString(in);
	}

}