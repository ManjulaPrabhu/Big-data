package assignment.topReviewCount;

import java.io.IOException;
import java.util.TreeMap;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TopReviewCountMapper extends Mapper<LongWritable,Text,NullWritable,Text>{
	private TreeMap<Integer,Text> intermediateMap=new TreeMap<Integer,Text>();
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, NullWritable,Text>.Context context)
			throws IOException, InterruptedException {
		int reviewCount;
		String subStringToUse;
		String[] fields=value.toString().split(",");
		if(key.get()==0) {
			return;
		}else {
		Text userID=new Text(fields[0]);
		String reviewCountString=String.valueOf(fields[2]);
		if(reviewCountString.length()>1) {
		 subStringToUse=reviewCountString.substring(1,reviewCountString.length()-1);
		}else {
			subStringToUse="0";
		}
		try {
		 reviewCount=Integer.parseInt(subStringToUse);
		 intermediateMap.put(reviewCount, new Text(value));
		 if(intermediateMap.size()>10) {
			 intermediateMap.remove(intermediateMap.firstKey());
		 }
		}
		catch(NumberFormatException e) {
			 reviewCount=1;
		}
		
		}
		
	}
	@Override
	protected void cleanup(Mapper<LongWritable, Text, NullWritable, Text>.Context context)
			throws IOException, InterruptedException {
		for(Text t:intermediateMap.values()) {
			context.write(NullWritable.get(), t);
		}
	}
}