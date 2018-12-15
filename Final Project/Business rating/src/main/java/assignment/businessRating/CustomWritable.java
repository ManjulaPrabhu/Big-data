package assignment.businessRating;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

public class CustomWritable implements Writable {
	String businessName;
	String businessCategory;
	String businessState;
	String postalCode;

	public String getPostalCode() {
		return postalCode;
	}

	public void setPostalCode(String postalCode) {
		this.postalCode = postalCode;
	}

	public String getBusinessState() {
		return businessState;
	}

	public void setBusinessState(String businessState) {
		this.businessState = businessState;
	}

	public String getBusinessCategory() {
		return businessCategory;
	}

	public void setBusinessCategory(String businessCategory) {
		this.businessCategory = businessCategory;
	}

	float rating;

	public float getRating() {
		return rating;
	}

	public void setRating(float rating) {
		this.rating = rating;
	}

	public String getBusinessName() {
		return businessName;
	}

	public void setBusinessName(String businessName) {
		this.businessName = businessName;
	}

	public void write(DataOutput out) throws IOException {
		WritableUtils.writeString(out, businessName);
		out.writeFloat(rating);
		WritableUtils.writeString(out, businessCategory);
		WritableUtils.writeString(out, businessState);
		WritableUtils.writeString(out, postalCode);

	}

	public void readFields(DataInput in) throws IOException {
		businessName = WritableUtils.readString(in);
		rating = in.readFloat();
		businessCategory = WritableUtils.readString(in);
		businessState = WritableUtils.readString(in);
		postalCode = WritableUtils.readString(in);
	}

	public String toString() {
		return businessName + "," + businessState + "," + postalCode + "," + businessCategory + "," + rating;
	}

}
