package assignment.secondarySorting;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class SecondarySortingReducer extends Reducer<CompositeKeyWritable,IntWritable,CompositeKeyWritable,IntWritable>{
IntWritable total=new IntWritable();
	@Override
	protected void reduce(CompositeKeyWritable key, Iterable<IntWritable> values,
			Reducer<CompositeKeyWritable, IntWritable, CompositeKeyWritable, IntWritable>.Context context) throws IOException, InterruptedException {
	int totalCount=0;
	for(IntWritable val:values) {
		totalCount+=val.get();
	}
	total.set(totalCount);
	context.write(key, total);
	}

}