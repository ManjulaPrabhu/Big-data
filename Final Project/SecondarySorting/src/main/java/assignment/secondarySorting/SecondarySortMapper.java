package assignment.secondarySorting;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SecondarySortMapper extends Mapper<LongWritable, Text, CompositeKeyWritable, NullWritable> {

	@Override
	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, CompositeKeyWritable, NullWritable>.Context context)
			throws IOException, InterruptedException {
		if (key.get() == 0) {
			return;
		} else {
			String[] fields = value.toString().split("\\|");
			String businessId = fields[0];
			String businessCity = fields[4];
			String businessName = fields[1];
			float starRating;

			if (fields[5].equals("AZ") && fields[12].toLowerCase().contains("dentists")
					&& Float.parseFloat(fields[9]) > 4) {
				try {
					starRating = Float.parseFloat(fields[9]);

				} catch (Exception e) {
					starRating = Float.parseFloat("0");

				}
				context.write(new CompositeKeyWritable(businessId, businessName, starRating, businessCity),
						NullWritable.get());
			}
		}
	}
}
