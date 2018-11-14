package assignment.stock;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class CountAverageTuple implements Writable{
int count;
float average;
public int getCount() {
	return count;
}
public void setCount(int count) {
	this.count = count;
}
public float getAverage() {
	return average;
}
public void setAverage(float average) {
	this.average = average;
}
public void write(DataOutput out) throws IOException {
	out.writeInt(count);
	out.writeFloat(average);
	
}
public void readFields(DataInput in) throws IOException {
	count=in.readInt();
	average=in.readFloat();
}
public String toString() {
	return count+"\t"+average;
}


}
