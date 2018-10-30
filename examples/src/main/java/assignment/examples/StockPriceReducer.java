package assignment.examples;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class StockPriceReducer extends Reducer<Text,FloatWritable,Text,FloatWritable> {
	FloatWritable maximumValue=new FloatWritable();
	@Override
	protected void reduce(Text key, Iterable<FloatWritable> values,
			Reducer<Text, FloatWritable, Text, FloatWritable>.Context context) throws IOException, InterruptedException {
	float maxStockPrice=0;
	for(FloatWritable val:values) {
		if(maxStockPrice<val.get()) {
			maxStockPrice=val.get();
		}
		}
	maximumValue.set(maxStockPrice);
	context.write(key, maximumValue);
	}
}
