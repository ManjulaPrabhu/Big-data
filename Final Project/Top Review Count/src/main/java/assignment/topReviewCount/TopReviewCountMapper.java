package assignment.topReviewCount;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TopReviewCountMapper extends Mapper<LongWritable,Text,Text,IntWritable>{
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text,IntWritable>.Context context)
			throws IOException, InterruptedException {
		IntWritable reviewCount;
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
		 reviewCount=new IntWritable(Integer.parseInt(subStringToUse));
		}
		catch(NumberFormatException e) {
			 reviewCount=new IntWritable(1);
		}
		context.write(userID, reviewCount);
		}
	}
}