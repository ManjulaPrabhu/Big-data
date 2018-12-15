package assignment.distributedGrep;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class DistributedGrepReducer extends Reducer<Text, BusinessCityTuple, NullWritable, Text> {
	int italianRestaurantsCount = 0;
	int indianRestaurantsCount = 0;
	int asianRestaurantsCount = 0;
	int mexicanRestaurantsCount = 0;

	@Override
	protected void reduce(Text key, Iterable<BusinessCityTuple> values,
			Reducer<Text, BusinessCityTuple, NullWritable, Text>.Context context)
			throws IOException, InterruptedException {
		for (BusinessCityTuple val : values) {
			if (val.getCategory().matches(".*Italian.Restaurants.*")) {
				italianRestaurantsCount++;
			} else if (val.getCategory().matches(".*Indian.Restaurants.*")) {
				indianRestaurantsCount++;
			} else if (val.getCategory().matches(".*Chinese.Restaurants.*")) {
				asianRestaurantsCount++;
			} else if (val.getCategory().matches(".*Mexican.Restaurants.*")) {
				mexicanRestaurantsCount++;
			}
		}

	}

	@Override
	protected void cleanup(Reducer<Text, BusinessCityTuple, NullWritable, Text>.Context context)
			throws IOException, InterruptedException {
		Text output = new Text(String.format("%s,%s,%s,%s", String.valueOf(italianRestaurantsCount),
				String.valueOf(indianRestaurantsCount), String.valueOf(asianRestaurantsCount),
				String.valueOf(mexicanRestaurantsCount)));
		context.write(NullWritable.get(), new Text("Italian, Indian, Chinese, Mexican"));
		context.write(NullWritable.get(), output);
	}
}
