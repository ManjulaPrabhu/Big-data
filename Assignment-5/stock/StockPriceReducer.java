package assignment.stock;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class StockPriceReducer extends Reducer<Text,CountAverageTuple,Text,CountAverageTuple> {
	CountAverageTuple result=new CountAverageTuple();
	@Override
	protected void reduce(Text key, Iterable<CountAverageTuple> values,
			Reducer<Text, CountAverageTuple, Text, CountAverageTuple>.Context context) throws IOException, InterruptedException {
	int count=0;
	float sum=0;
	for(CountAverageTuple val:values) {
		sum+=val.getCount()*val.getAverage();
		count+=val.getCount();
		}
	result.setCount(count);
	result.setAverage(sum/count);
	context.write(key, result);
	}
}

