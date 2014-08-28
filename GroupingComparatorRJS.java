package reduceSide;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class GroupingComparatorRJS extends WritableComparator{

	protected GroupingComparatorRJS()
	{
		super(RSJcompositeKey.class, true);
	}
	@Override
	public int compare(WritableComparable w1, WritableComparable w2) {
		// The grouping comparator is the joinKey (Employee ID)
		RSJcompositeKey key1 = (RSJcompositeKey) w1;
		RSJcompositeKey key2 = (RSJcompositeKey) w2;
		return key1.getJoinKey().compareTo(key2.getJoinKey());
	}
}
