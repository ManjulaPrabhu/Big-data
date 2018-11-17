package assignment.secondarySorting;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class GroupingComparator extends WritableComparator{
	GroupingComparator(){
		super(CompositeKeyWritable.class,true);
	}

	@Override
	public int compare(WritableComparable w1, WritableComparable w2) {
		CompositeKeyWritable k1=(CompositeKeyWritable)w1;
		CompositeKeyWritable k2=(CompositeKeyWritable)w2;
		
		return k1.getIpAddress().compareTo(k2.getIpAddress());
		
	}

}
