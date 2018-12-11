package assignment.checkinAnalysis;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;



public class CheckInAnalysisReducer extends Reducer<Text,IntWritable,Text,IntWritable> {

	@Override
	protected void reduce( Text key, Iterable<IntWritable> values,
			Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
		int totalCheckInCount=0;
		for(IntWritable val:values) {
		totalCheckInCount+=val.get();
		
		}
		context.write(key,new IntWritable(totalCheckInCount));
}
}