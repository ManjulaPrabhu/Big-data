package assignment.secondarySorting;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

public class CompositeKeyWritable implements Writable, WritableComparable<CompositeKeyWritable> {
	String businessId;
	String businessName;
	float starRating;
	String businessCity;

	CompositeKeyWritable() {

	}

	public String getBusinessName() {
		return businessName;
	}

	public void setBusinessName(String businessName) {
		this.businessName = businessName;
	}

	CompositeKeyWritable(String businessId, String businessName, float starRating, String businessCity) {
		this.businessId = businessId;
		this.businessName = businessName;
		this.starRating = starRating;
		this.businessCity = businessCity;
	}

	public String getBusinessCity() {
		return businessCity;
	}

	public void setBusinessCity(String businessCity) {
		this.businessCity = businessCity;
	}

	public String getBusinessId() {
		return businessId;
	}

	public void setBusinessId(String businessId) {
		this.businessId = businessId;
	}

	public float getStarRating() {
		return starRating;
	}

	public void setStarRating(float starRating) {
		this.starRating = starRating;
	}

	public void write(DataOutput out) throws IOException {
		WritableUtils.writeString(out, businessId);
		WritableUtils.writeString(out, businessName);
		WritableUtils.writeString(out, businessCity);
		out.writeFloat(starRating);

	}

	public void readFields(DataInput in) throws IOException {
		businessId = WritableUtils.readString(in);
		businessName = WritableUtils.readString(in);
		businessCity = WritableUtils.readString(in);
		starRating = in.readFloat();

	}

	public String toString() {
		return businessId + "," + businessName + "," + businessCity + "," + starRating;
	}

	public int compareTo(CompositeKeyWritable objectPair) {
		int result = businessCity.compareTo(objectPair.businessCity);
		if (result == 0) {
			result = Float.compare(starRating, objectPair.getStarRating());
		}
		return result;
	}

}
