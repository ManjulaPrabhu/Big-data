package assignment.stock;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class StockPriceMapper extends Mapper<LongWritable,Text,Text,FloatWritable> {
	IntWritable one=new IntWritable(1);
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, FloatWritable>.Context context)
			throws IOException, InterruptedException {
		try {
		String[] fields=value.toString().split(",");
		Text t=new Text(fields[0]);
		FloatWritable valueToWrite=new FloatWritable(Float.parseFloat(fields[4]));
		context.write(t, valueToWrite);
		}
		catch(Exception e) {
			System.out.println(e.getMessage());
		}
	}
}
