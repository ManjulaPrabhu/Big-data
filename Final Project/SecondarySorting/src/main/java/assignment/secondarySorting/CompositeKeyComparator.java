package assignment.secondarySorting;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class CompositeKeyComparator extends WritableComparator {
	CompositeKeyComparator() {
		super(CompositeKeyWritable.class, true);
	}

	@Override
	public int compare(WritableComparable w1, WritableComparable w2) {
		CompositeKeyWritable k1 = (CompositeKeyWritable) w1;
		CompositeKeyWritable k2 = (CompositeKeyWritable) w2;

		int compareResult = Integer.compare(k1.getReviewCount(), k2.getReviewCount());
		if (compareResult == 0) {
			return -1 * Float.compare(k1.getStarRating(), k2.getStarRating());
		}
		return compareResult;
	}
}
