package assignment.joinAnalysis;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class JoinAnalysisReducer extends Reducer<Text,Text,Text,Text> {
private List<Text> listA=new ArrayList<Text>();
private List<Text> listB=new ArrayList<Text>();

	@Override
	protected void reduce( Text key, Iterable<Text> values,
			Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
		listA.clear();
		listB.clear();
		for(Text val:values) {
			if(val.charAt(1)=='A') {
				listA.add(new Text(val.toString().substring(1)));
			}else if(val.charAt(1)=='B') {
				listB.add(new Text(val.toString().substring(1)));
			}
		}
		//performing inner join
		if(!listA.isEmpty() && !listB.isEmpty()) {
			for(Text A: listA) {
				for(Text B:listB) {
					context.write(A, B);
				}
			}
		}
	
}
}
