package assignment.topReviewCount;

import java.io.IOException;
import java.util.Collections;
import java.util.NavigableMap;
import java.util.TreeMap;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TopReviewCountReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
TreeMap<Integer,Text> intermediateMap=new TreeMap<Integer,Text>();

	@Override
	protected void reduce(Text key, Iterable<IntWritable> values,
			Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
		int totalReviewCount=0;
		for(IntWritable eachReviewCount:values) {
			totalReviewCount+=Integer.parseInt(String.valueOf(eachReviewCount));
		}
		intermediateMap.put(totalReviewCount,key);
		if(intermediateMap.size()>10) {
			intermediateMap.remove(intermediateMap.firstKey());
		}
	}
	
	@Override
	protected void cleanup(Reducer<Text, IntWritable, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
	NavigableMap<Integer,Text> descendingOrderMap=intermediateMap.descendingMap();
	for(NavigableMap.Entry<Integer,Text> entry:descendingOrderMap.entrySet()) {
		context.write(entry.getValue(), new IntWritable(entry.getKey()));
	}
	}

	
}

