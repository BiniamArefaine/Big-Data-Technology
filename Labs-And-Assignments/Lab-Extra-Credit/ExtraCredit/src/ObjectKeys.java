

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;



@SuppressWarnings("rawtypes")
public class ObjectKeys implements WritableComparable  {

   private String stationId_temp;
    
   private String id;
   private String temp;	
	

	public String getId() {
	return id;
}


public void setId(String id) {
	this.id = id;
}


public String getTemp() {
	return temp;
}



public void setTemp(String temp) {
	this.temp = temp;
}



	public ObjectKeys(String stationId_temp) {
	this.stationId_temp=stationId_temp;
	}



	public ObjectKeys() {}

	@Override
	public int compareTo(Object obj) {
		this.setId(this.stationId_temp.split(",")[0]);  
		this.setTemp(this.stationId_temp.split(",")[1]);
		
		ObjectKeys key=(ObjectKeys) obj;
		key.setId(key.stationId_temp.split(",")[0]);
		key.setTemp(key.stationId_temp.split(",")[1]);
		
		
		if(key.getId().equals(this.getId())) {
			return  Integer.compare(Integer.parseInt(key.getTemp()),Integer.parseInt(this.getTemp()));
		}
		return -key.getId().compareTo(this.getId());
		
	
	}

	@Override
	public String toString() {
		return stationId_temp;
	}

	

	public String getStationId_temp() {
		return stationId_temp;
	}

	public void setStationId_temp(String stationId_temp) {
		this.stationId_temp = stationId_temp;
	}


	@Override
	public void readFields(DataInput arg0) throws IOException {
		
		stationId_temp=arg0.readLine();
		
	}



	@Override
	public void write(DataOutput arg0) throws IOException {
		arg0.writeBytes(stationId_temp);
		
		
	}
	
  }