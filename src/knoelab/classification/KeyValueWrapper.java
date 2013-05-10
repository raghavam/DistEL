package knoelab.classification;

import java.util.ArrayList;
import java.util.List;

public class KeyValueWrapper {
	List<String> keyList;
	List<String> valueList;
	
	KeyValueWrapper() {
		keyList = new ArrayList<String>();
		valueList = new ArrayList<String>();
	}
	
	void addToKeyValueList(String k, String v) {
		keyList.add(k);
		valueList.add(v);
	}
	
	int getSize() {
		return keyList.size();
	}
	
	void clear() {
		keyList.clear();
		valueList.clear();
	}
}
