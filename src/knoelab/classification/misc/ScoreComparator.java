package knoelab.classification.misc;

import java.util.Comparator;
import java.util.Map;

public class ScoreComparator<V extends Comparable<V>> 
	implements Comparator<Map.Entry<?, V>> {
	
	public int compare(Map.Entry<?, V> o1, 
			Map.Entry<?, V> o2) {
		return o1.getValue().compareTo(o2.getValue());
	}	
}
