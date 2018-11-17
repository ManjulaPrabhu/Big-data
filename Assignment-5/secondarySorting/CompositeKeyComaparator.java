package assignment.secondarySorting;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class CompositeKeyComaparator extends WritableComparator{
	CompositeKeyComaparator(){
		super(CompositeKeyWritable.class,true);
	}

	@Override
	public int compare(WritableComparable w1, WritableComparable w2) {
		CompositeKeyWritable k1=(CompositeKeyWritable)w1;
		CompositeKeyWritable k2=(CompositeKeyWritable)w2;
		
		int compareResult=k1.getIpAddress().compareTo(k2.getIpAddress());
		if(compareResult==0) {
			compareResult=-1*k1.getIpAccessDate().compareTo(k2.getIpAccessDate());
		}
		return compareResult;
	}
	

}
