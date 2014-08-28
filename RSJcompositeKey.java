package reduceSide;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

public class RSJcompositeKey implements Writable, WritableComparable<RSJcompositeKey>{

	
	private String joinKey;
	private int sourceIndex;
	
	
	
	public RSJcompositeKey() {
	}
 
	public RSJcompositeKey(String joinKey, int sourceIndex) {
		this.joinKey = joinKey;
		this.sourceIndex = sourceIndex;
	}
 
	@Override
	public String toString() {
 
		return (new StringBuilder().append(joinKey).append("\t")
				.append(sourceIndex)).toString();
	}
 
	@Override
	public int compareTo(RSJcompositeKey arg0) {

	
		int result = joinKey.compareTo(arg0.joinKey);
		if (0 == result) 
		{
			result = Double.compare(sourceIndex, arg0.sourceIndex);
		}
		return result;
	}

	public String getJoinKey() {
		return joinKey;
	}

	public void setJoinKey(String joinKey) {
		this.joinKey = joinKey;
	}

	public int getSourceIndex() {
		return sourceIndex;
	}

	public void setSourceIndex(int sourceIndex) {
		this.sourceIndex = sourceIndex;
	}

	@Override
	public void readFields(DataInput arg0) throws IOException {
		// TODO Auto-generated method stub
		joinKey=WritableUtils.readString(arg0);
		sourceIndex=WritableUtils.readVInt(arg0);
	}

	@Override
	public void write(DataOutput arg0) throws IOException {
		// TODO Auto-generated method stub
		WritableUtils.writeString(arg0, joinKey);
		WritableUtils.writeVInt(arg0, sourceIndex);
	}

}
