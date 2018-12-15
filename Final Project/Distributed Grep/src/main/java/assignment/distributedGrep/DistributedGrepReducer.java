package assignment.distributedGrep;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class DistributedGrepReducer extends Reducer<Text, BusinessCityTuple, NullWritable, Text> {
	int italianRestaurantsCount = 0;
	int indianRestaurantsCount = 0;

	@Override
	protected void reduce(Text key, Iterable<BusinessCityTuple> values,
			Reducer<Text, BusinessCityTuple, NullWritable, Text>.Context context)
			throws IOException, InterruptedException {
		for (BusinessCityTuple val : values) {
			if (val.getCategory().matches(".*Italian.Restaurants.*")) {
				italianRestaurantsCount++;
			} else if (val.getCategory().matches(".*Indian.Restaurants.*")) {
				indianRestaurantsCount++;
			}
		}

	}

	@Override
	protected void cleanup(Reducer<Text, BusinessCityTuple, NullWritable, Text>.Context context)
			throws IOException, InterruptedException {
		Text output = new Text(String.format("%s,%s", String.valueOf(italianRestaurantsCount),
				String.valueOf(indianRestaurantsCount)));
		context.write(NullWritable.get(), output);
	}
}
