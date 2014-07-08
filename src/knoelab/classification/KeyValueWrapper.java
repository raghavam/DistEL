package knoelab.classification;

import java.util.ArrayList;
import java.util.List;

public class KeyValueWrapper {
	private List<String> keyList;
	private List<String> valueList;
	
	public KeyValueWrapper() {
		keyList = new ArrayList<String>();
		valueList = new ArrayList<String>();
	}
	
	public void addToKeyValueList(String k, String v) {
		keyList.add(k);
		valueList.add(v);
	}
	
	public List<String> getKeyList() {
		return keyList;
	}
	
	public List<String> getValueList() {
		return valueList;
	}
	
	public int getSize() {
		return keyList.size();
	}
	
	public void clear() {
		keyList.clear();
		valueList.clear();
	}
}
