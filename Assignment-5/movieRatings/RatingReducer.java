package assignment.movieRatings;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class RatingReducer extends Reducer<Text,FloatWritable,Text,MedianStdDevTuple> {
private MedianStdDevTuple result=new MedianStdDevTuple();
private List<Float> ratingsList=new ArrayList<Float>();
	@Override
	protected void reduce(Text key, Iterable<FloatWritable> values,
			Reducer<Text, FloatWritable, Text, MedianStdDevTuple>.Context context) throws IOException, InterruptedException {
		float sum=0;
		float count=0;
		
		result.setMedian(0);
		result.setStdDev(0);
		
		for(FloatWritable eachValue:values) {
			ratingsList.add(eachValue.get());
			sum+=eachValue.get();
			++count;
		}
		
		Collections.sort(ratingsList);
		if(count%2==0) {
			result.setMedian((ratingsList.get((int)count/2-1)+ratingsList.get((int)count/2))/2.0f);
			
		}else {
			result.setMedian(ratingsList.get((int)count/2));
		}
		
		float mean=sum/count;
		float sumOfSquares=0.0f;
		for(Float f: ratingsList) {
			sumOfSquares+=(f-mean)*(f-mean);
		}
		
		result.setStdDev((float)Math.sqrt(sumOfSquares/(count-1)));
		context.write(key,result);
	}
	

}
