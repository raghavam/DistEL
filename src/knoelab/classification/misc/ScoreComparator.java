package knoelab.classification.misc;

import java.util.Comparator;
import java.util.Map;

public class ScoreComparator<V extends Comparable<V>> 
	implements Comparator<Map.Entry<?, V>> {
	
	private boolean isAscending;
	
	public ScoreComparator(boolean isAscending) {
		this.isAscending = isAscending;
	}
	
	public int compare(Map.Entry<?, V> o1, 
			Map.Entry<?, V> o2) {
		if(isAscending)
			return o1.getValue().compareTo(o2.getValue());
		else
			return (o1.getValue().compareTo(o2.getValue())) * -1;
	}	
}
