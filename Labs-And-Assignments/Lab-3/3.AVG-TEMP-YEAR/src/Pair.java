import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;


	public class Pair implements Writable{
	private int temp;
	private int count;
	
	public Pair(){	
	}
	public Pair(int temp, int count) {
		super();
		this.temp = temp;
		this.count = count;
	}
	public int getTemp() {
		return temp;
	}
	public void setTemp(int temp) {
		this.temp = temp;
	}
	public int getCount() {
		return count;
	}
	public void setCount(int count) {
		this.count = count;
	}
	@Override
	public void readFields(DataInput arg0) throws IOException {
		count = arg0.readInt();
		temp = arg0.readInt();		
	}
	@Override
	public void write(DataOutput arg0) throws IOException {
		arg0.writeInt(count);
		arg0.writeInt(temp);
		
	}
	
	}
