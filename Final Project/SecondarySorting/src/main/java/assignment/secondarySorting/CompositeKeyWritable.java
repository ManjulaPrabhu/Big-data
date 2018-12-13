package assignment.secondarySorting;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

public class CompositeKeyWritable implements Writable, WritableComparable<CompositeKeyWritable> {
	String businessId;
	float starRating;
	int reviewCount;

	CompositeKeyWritable() {

	}

	CompositeKeyWritable(String businessId, float starRating, int reviewCount) {
		this.businessId = businessId;
		this.starRating = starRating;
		this.reviewCount = reviewCount;
	}

	public int getReviewCount() {
		return reviewCount;
	}

	public void setReviewCount(int reviewCount) {
		this.reviewCount = reviewCount;
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
		out.writeFloat(starRating);
		out.writeInt(reviewCount);
	}

	public void readFields(DataInput in) throws IOException {
		businessId = WritableUtils.readString(in);
		starRating = in.readFloat();
		reviewCount = in.readInt();

	}

	public String toString() {
		return businessId + "\t" + reviewCount + "\t" + starRating;
	}

	public int compareTo(CompositeKeyWritable objectPair) {
		int result = Integer.compare(reviewCount, objectPair.getReviewCount());
		if (result == 0) {
			result = Float.compare(starRating, objectPair.getStarRating());
		}
		return result;
	}

}
