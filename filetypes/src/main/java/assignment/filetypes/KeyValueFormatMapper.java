package assignment.filetypes;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class KeyValueFormatMapper extends Mapper<Text,Text,Text,IntWritable>{
	@Override
	protected void map(Text key, Text value, Mapper<Text, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		Text t=new Text(key);
		try {
		System.out.println(value.toString());
		context.write(t, new IntWritable(Integer.parseInt(value.toString())));
		}
		catch(Exception e) {
			System.out.println(e.getMessage());
		}
		}
		

}
