package assignment.distributedGrep;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

public class BusinessCityTuple implements Writable {
	String businessCity;
	String category;

	public String getCategory() {
		return category;
	}

	public void setCategory(String category) {
		this.category = category;
	}

	public String getBusinessCity() {
		return businessCity;
	}

	public void setBusinessCity(String businessCity) {
		this.businessCity = businessCity;
	}

	public String toString() {
		return "BUSINESS CITY " + businessCity + "\t" + "BUSINESS CATEGORY " + category;

	}

	public void write(DataOutput out) throws IOException {
		WritableUtils.writeString(out, businessCity);
		WritableUtils.writeString(out, category);

	}

	public void readFields(DataInput in) throws IOException {
		businessCity = WritableUtils.readString(in);
		category = WritableUtils.readString(in);
	}

}