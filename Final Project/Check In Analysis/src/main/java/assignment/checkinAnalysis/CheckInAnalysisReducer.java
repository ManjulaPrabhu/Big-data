package assignment.checkinAnalysis;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CheckInAnalysisReducer extends Reducer<Text, Text, Text, Text> {

	@Override
	protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		Map<String, Integer> checkinMapByDay = new HashMap();
		for (Text val : values) {
			String[] dayAndCheckin = val.toString().split(":");
			if (!checkinMapByDay.containsKey(dayAndCheckin[0])) {
				checkinMapByDay.put(dayAndCheckin[0], Integer.parseInt(dayAndCheckin[1]));
			} else {
				int checkinCount = checkinMapByDay.get(dayAndCheckin[0]);
				checkinMapByDay.put(dayAndCheckin[0], checkinCount + Integer.parseInt(dayAndCheckin[1]));
			}
		}
		int maxCheckinByDay = Collections.max(checkinMapByDay.values());
		for (Entry<String, Integer> entry : checkinMapByDay.entrySet()) {
			if (entry.getValue() == maxCheckinByDay) {
				context.write(key, new Text("," + entry.getKey()));
				break;
			}
		}
	}
}