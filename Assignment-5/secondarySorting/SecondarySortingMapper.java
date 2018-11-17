package assignment.secondarySorting;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SecondarySortingMapper extends Mapper<LongWritable,Text,CompositeKeyWritable,IntWritable>{

@Override
protected void map(LongWritable key, Text value,
		Mapper<LongWritable, Text, CompositeKeyWritable, IntWritable>.Context context)
		throws IOException, InterruptedException {
	String[] fields=value.toString().split(" ");
	Text t=new Text(fields[0]);
	
	try {
		String accessDate=fields[3].substring(1, fields[3].indexOf(":"));
		System.out.println("debug "+accessDate);
		SimpleDateFormat format=new SimpleDateFormat("dd/MMM/yyyy");
		Date dateToProcess=format.parse(accessDate.trim());
		System.out.println(dateToProcess);
		context.write(new CompositeKeyWritable(fields[0],dateToProcess), new IntWritable(1));
	} catch (ParseException e) {
		System.out.println(e);
	}
}
}

