package assignment.distributedGrep;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class DistributedGrepReducer extends Reducer<Text,BusinessCityTuple,Text,BusinessCityTuple> {

	@Override
	protected void reduce( Text key, Iterable<BusinessCityTuple> values,
			Reducer<Text, BusinessCityTuple, Text, BusinessCityTuple>.Context context) throws IOException, InterruptedException {
	for(BusinessCityTuple val:values) {
		context.write(key, val);
	}
	
}
}

