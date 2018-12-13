package assignment.topReviewCount;

import java.io.IOException;
import java.util.TreeMap;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TopReviewCountReducer extends Reducer<NullWritable,Text,IntWritable,Text> {
TreeMap<Integer,Text> intermediateMap=new TreeMap<Integer,Text>();

	@Override
	protected void reduce(NullWritable key, Iterable<Text> values,
			Reducer<NullWritable, Text, IntWritable, Text>.Context context) throws IOException, InterruptedException {
		int totalReviewCount=0;
		for(Text eachReviewCount:values) {
			String[] fields=eachReviewCount.toString().split(",");
			System.out.println(fields[2]);
			totalReviewCount+=Integer.parseInt(fields[2].substring(1, fields[2].length() - 1));
		
			intermediateMap.put(totalReviewCount,new Text(eachReviewCount));
			if(intermediateMap.size()>10) {
				intermediateMap.remove(intermediateMap.firstKey());
			}
		}
		for(Integer count:intermediateMap.descendingKeySet()) {
			String[] stringToOutput=intermediateMap.get(count).toString().split(",");
			String outputString=stringToOutput[0];
			context.write(new IntWritable(count), new Text(outputString));
		}
}
}

