package assignment.ipCount;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class IpCountMapper extends Mapper<LongWritable,Text,Text,IntWritable>{
	IntWritable one=new IntWritable(1);
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		String[] fields=value.toString().split(" ");
		for(String s:fields) {
			Text t=new Text(s);
			context.write(t, one);
		}
	
	}


}
