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
			float starRating;
			int reviewCount;
			try {
				starRating = Float.parseFloat(fields[9]);
				reviewCount = Integer.parseInt(fields[10]);
			} catch (Exception e) {
				starRating = Float.parseFloat("0");
				reviewCount = Integer.parseInt("0");
			}
			context.write(new CompositeKeyWritable(businessId, starRating, reviewCount), NullWritable.get());

		}
	}
}
