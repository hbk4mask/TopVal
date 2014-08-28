package reduceSide;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class PartitionerRSJ extends Partitioner<RSJcompositeKey, Text>

{

	@Override
	public int getPartition(RSJcompositeKey key, Text value, int numReduceTasks) {
		// TODO Auto-generated method stub
		return (key.getJoinKey().hashCode()%numReduceTasks);
		
	}

}
