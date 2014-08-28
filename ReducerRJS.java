package reduceSide;

import java.io.File;
import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;

public class ReducerRJS extends Reducer<RSJcompositeKey, Text, NullWritable, Text> {

	StringBuilder reduceValueBuilder = new StringBuilder("");
	NullWritable nullWritableKey = NullWritable.get();
	Text reduceOutputValue = new Text("");
	String strSeparator = ",";
	//private MapFile.Reader deptMapReader = null;
	Text txtMapFileLookupKey = new Text("");
	Text txtMapFileLookupValue = new Text("");

	private StringBuilder buildOutputValue(RSJcompositeKey key,	StringBuilder reduceValueBuilder, Text value) {

		if (key.getSourceIndex() == 1) 
		{
			// Employee data
			// {{
			// Get the department name from the MapFile in distributedCache

			// Insert the joinKey (empNo) to beginning of the stringBuilder
			reduceValueBuilder.append(key.getJoinKey()).append(strSeparator);

			String arrEmpAttributes[] = value.toString().split(",");
			
			reduceValueBuilder.append(value.toString()).append(strSeparator);
		

		}
		
		else if (key.getSourceIndex() == 2) {
			// Current recent salary data (1..1 on join key)
			// Salary data; Just append the salary, drop the effective-to-date
			String arrSalAttributes[] = value.toString().split(",");
			reduceValueBuilder.append(arrSalAttributes[0].toString()).append(strSeparator);
			reduceValueBuilder.append(arrSalAttributes[1].toString()).append(strSeparator);
		} 
		
		else // key.getsourceIndex() == 3; Historical salary data
		{
			// {{
			// Get the salary data but extract only current salary
			// (to_date='9999-01-01')
			String arrSalAttributes[] = value.toString().split(",");
		
				//reduceValueBuilder.append(arrSalAttributes[0].toString()).append(strSeparator);
				//reduceValueBuilder.append(arrSalAttributes[1].toString()).append(strSeparator);
			
			// }}

		}

		// {{
		// Reset
		txtMapFileLookupKey.set("");
		txtMapFileLookupValue.set("");
		// }}

		return reduceValueBuilder;
	}

	@Override
	public void reduce(RSJcompositeKey key, Iterable<Text> values,Context context) throws IOException, InterruptedException {

		// Iterate through values; First set is csv of employee data
		// second set is salary data; The data is already ordered
		// by virtue of secondary sort; Append each value;
		for (Text value : values)
		{
			buildOutputValue(key, reduceValueBuilder, value);
			//context.write(nullWritableKey, value);
		}

		// Drop last comma, set value, and emit output
		if (reduceValueBuilder.length() > 1) 
		{

			reduceValueBuilder.setLength(reduceValueBuilder.length() - 1);
			// Emit output
			reduceOutputValue.set(reduceValueBuilder.toString());
			context.write(nullWritableKey, reduceOutputValue);
		} 
		else
		
		{
			System.out.println("Key=" + key.getJoinKey() + "src="+ key.getSourceIndex());

		}

		// Reset variables
		reduceValueBuilder.setLength(0);
		reduceOutputValue.set("");

	}

}