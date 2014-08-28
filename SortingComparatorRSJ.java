package reduceSide;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class SortingComparatorRSJ extends WritableComparator {
	
	protected SortingComparatorRSJ()
	{
		super(RSJcompositeKey.class, true);	
		
	}
	@Override
	public int compare(WritableComparable a, WritableComparable b) 	{
		
		RSJcompositeKey key1=(RSJcompositeKey) a;
		RSJcompositeKey key2=(RSJcompositeKey) b;
		
		int cmpResults=key1.getJoinKey().compareTo(key2.getJoinKey());
		
		if(cmpResults==0)
		{
			return Double.compare(key1.getSourceIndex(), key2.getSourceIndex());	
			
		}
		return cmpResults;
	}

}
