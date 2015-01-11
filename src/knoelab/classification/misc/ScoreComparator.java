package knoelab.classification.misc;

import java.util.Comparator;
import java.util.Map;

public class ScoreComparator 
	implements Comparator<Map.Entry<?, Double>> {
	
	public int compare(Map.Entry<?, Double> o1, 
			Map.Entry<?, Double> o2) {
		return o1.getValue().compareTo(o2.getValue());
	}	
}
