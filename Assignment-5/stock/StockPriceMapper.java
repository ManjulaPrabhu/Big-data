package assignment.stock;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class StockPriceMapper extends Mapper<LongWritable,Text,Text,CountAverageTuple> {
	CountAverageTuple countAverageObject=new CountAverageTuple();
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, CountAverageTuple>.Context context)
			throws IOException, InterruptedException {
		try {
		String[] fields=value.toString().split(",");
		String[] yearPart=fields[2].split("-");
		
		Text t=new Text(yearPart[0]);
		countAverageObject.setCount(1);
		countAverageObject.setAverage(Float.parseFloat(fields[8]));
		context.write(t, countAverageObject);
		}
		catch(Exception e) {
			System.out.println(e.getMessage());
		}
	}
}
