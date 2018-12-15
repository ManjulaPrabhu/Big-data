package assignment.distributedGrep;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class DistributedGrepMapper extends Mapper<LongWritable, Text, Text, BusinessCityTuple> {
	BusinessCityTuple outputObject = new BusinessCityTuple();

	@Override
	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, Text, BusinessCityTuple>.Context context)
			throws IOException, InterruptedException {
		String[] fields = value.toString().split(",");
		if (key.get() == 0) {
			return;
		} else if ((fields[12].matches(".*Italian.Restaurants.*") && fields[4].equals("Toronto"))
				|| (fields[12].matches(".*Indian.Restaurants.*") && fields[4].equals("Toronto"))) {
			Text businessId = new Text(fields[0]);
			outputObject.setBusinessCity(fields[4]);
			outputObject.setCategory(fields[12]);

			context.write(businessId, outputObject);
		}
	}
}
