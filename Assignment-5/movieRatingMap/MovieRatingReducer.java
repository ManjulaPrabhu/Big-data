package assignment.movieRatingMap;

import java.io.IOException;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SortedMapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Reducer;

public class MovieRatingReducer extends Reducer<Text,SortedMapWritable,Text,MedianStdDevTuple> {
	private MedianStdDevTuple result=new MedianStdDevTuple();
	private TreeMap<Float,Long> ratingFrequencyCount=new TreeMap<Float,Long>();
	
	
		@Override
		protected void reduce(Text key, Iterable<SortedMapWritable> values,
				Reducer<Text, SortedMapWritable, Text, MedianStdDevTuple>.Context context) throws IOException, InterruptedException {
			float sum=0;
			long totalRatings=0;
			ratingFrequencyCount.clear();
			result.setMedian(0);
			result.setStdDev(0);
			
			for(SortedMapWritable v:values) {
				for(Entry<WritableComparable,Writable> entry:v.entrySet()) {
					float rating=((FloatWritable)entry.getKey()).get();
					long count=((LongWritable)entry.getValue()).get();
					
					totalRatings+=count;
					sum+=rating*count;
					
					Long storedCount=ratingFrequencyCount.get(rating);
					if(storedCount==null) {
						ratingFrequencyCount.put(rating,count);
					}else {
						ratingFrequencyCount.put(rating,storedCount+count);
					}
				}
			}
			
			long medianIndex=totalRatings/2L;
			long previousRating=0;
			long ratings=0;
			float prevKey=0;
			
			for(Entry<Float,Long> entry: ratingFrequencyCount.entrySet()) {
				ratings=previousRating+entry.getValue();
				
				if(previousRating<=medianIndex && medianIndex<ratings) {
					if(totalRatings%2==0 && previousRating==medianIndex) {
						
						result.setMedian((float)(entry.getKey()+prevKey)/2.0f);
					}else {
						
						result.setMedian(entry.getKey());
					}
					break;
				}
				previousRating=ratings;
				prevKey=entry.getKey();
				
			}
			
			
			float mean=sum/totalRatings;
			float sumOfSquares=0.0f;
			
			for(Entry<Float,Long> entry: ratingFrequencyCount.entrySet()) {
				sumOfSquares+=(entry.getKey()-mean)*(entry.getKey()-mean)*entry.getValue();
				
			}
			result.setStdDev((float)Math.sqrt(sumOfSquares/(totalRatings-1)));
			context.write(key,result);
		}
}
