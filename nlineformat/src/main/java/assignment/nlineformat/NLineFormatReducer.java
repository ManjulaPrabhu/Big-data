package assignment.nlineformat;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class NLineFormatReducer extends Reducer<Text,IntWritable,Text,IntWritable>{
IntWritable total=new IntWritable();
	@Override
	protected void reduce(Text key, Iterable<IntWritable> values,
			Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
	int totalCount=0;
	for(IntWritable val:values) {
		totalCount+=val.get();
	}
	total.set(totalCount);
	context.write(key, total);
	}

}
