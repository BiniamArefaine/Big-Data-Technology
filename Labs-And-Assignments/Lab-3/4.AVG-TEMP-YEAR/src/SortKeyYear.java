
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

@SuppressWarnings("rawtypes")
public class SortKeyYear implements WritableComparable{
	
	String year;
	
	public SortKeyYear() {
		super();
	}

	public SortKeyYear(String year) {
		super();
		this.year = year;
	}

	public String getYear() {
		return year;
	}

	public void setYear(String year) {
		this.year = year;
	}

	@Override
	public void readFields(DataInput arg0) throws IOException {
		
		year = arg0.readLine();
	}

	@Override
	public void write(DataOutput arg0) throws IOException {
		
		arg0.writeBytes(year);
	}

	@Override
	public int compareTo(Object obj) {
		
		return this.year.compareTo(((SortKeyYear)obj).toString());
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((year == null) ? 0 : year.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		SortKeyYear other = (SortKeyYear) obj;
		if (year == null) {
			if (other.year != null)
				return false;
		} else if (!year.equals(other.year))
			return false;
		return true;
	}

}
