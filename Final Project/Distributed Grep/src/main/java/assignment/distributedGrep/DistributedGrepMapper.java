package assignment.distributedGrep;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class DistributedGrepMapper extends Mapper<LongWritable,Text,Text,BusinessCityTuple>{
	BusinessCityTuple outputObject=new BusinessCityTuple();
@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text,Text,BusinessCityTuple>.Context context)
			throws IOException, InterruptedException {
		String[] fields=value.toString().split(",");
		if(key.get()==0) {
			return;
		}else if((fields[12].matches(".*Italian.*") && fields[5].equals("ON")) || (fields[12].matches(".*Coffee.*") && fields[5].equals("AZ"))) {
			Text businessId=new Text(fields[0]);
			outputObject.setBusinessName(fields[1]);	
			outputObject.setBusinessCity(fields[4]);
			outputObject.setCategory(fields[12]);
			
			context.write(businessId,outputObject);
		}
		}
	}
	

