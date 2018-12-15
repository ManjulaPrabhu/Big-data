package assignment.topReviewCount;

import java.io.IOException;
import java.util.TreeMap;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TopReviewerCountReducer extends Reducer<NullWritable, Text, NullWritable, Text> {
	TreeMap<Integer, Text> intermediateMap = new TreeMap<Integer, Text>();

	@Override
	protected void reduce(NullWritable key, Iterable<Text> values,
			Reducer<NullWritable, Text, NullWritable, Text>.Context context) throws IOException, InterruptedException {
		int totalReviewCount = 0;
		for (Text eachReviewCount : values) {
			String[] fields = eachReviewCount.toString().split(",");
			totalReviewCount += Integer.parseInt(fields[2].substring(1, fields[2].length() - 1));

			intermediateMap.put(totalReviewCount, new Text(eachReviewCount));
			if (intermediateMap.size() > 10) {
				intermediateMap.remove(intermediateMap.firstKey());
			}
		}
		for (Text val : intermediateMap.descendingMap().values()) {
			String[] properties = val.toString().split(",");
			context.write(NullWritable.get(), new Text(String.format("%s,%s", properties[1], properties[2])));
		}
	}
}
