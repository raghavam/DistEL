package knoelab.classification;

import java.util.Set;

import knoelab.classification.misc.AxiomDB;
import redis.clients.jedis.Jedis;

public class DataStats {

	private Jedis localStore = new Jedis("localhost", 6379);
	
	public void printAvgResultSize() throws Exception {
		try {
			Set<String> allConcepts = localStore.keys("*");
			System.out.println("No of concepts: " + allConcepts.size());
			int max = -1;
			double totalCount = 0;
			int currentQueueSize = 0;
			int error = 0;
			for(String key : allConcepts) {
				if(key.equals("type") || key.equals("TypeHost"))
					continue;
				try {
				currentQueueSize = localStore.zcard(key).intValue();
				}
				catch(Exception e) { currentQueueSize = 0; error++; }
				totalCount += currentQueueSize;
				if(max < currentQueueSize)
					max = currentQueueSize;
			}
			System.out.println("Avg result queue size: " + (totalCount/(allConcepts.size()-error)));
			System.out.println("Max result queue size: " + max);
		} 
		finally {
			localStore.disconnect();
		}
	}
	
	public void printRolePairs() throws Exception {
		try {
			// all axioms of type R(r) = {(X,Y), (P,Q), ....} would be in DB-1
			localStore.select(AxiomDB.ROLE_DB.ordinal());
			byte[] allKeys = new String("*").getBytes("UTF-8");
			Set<byte[]> allRoles = localStore.keys(allKeys);
			System.out.println("Total no of R(r)s: " + allRoles.size());
			int max = -1;
			double totalCount = 0;
			int currentQueueSize = 0;
			for(byte[] role : allRoles) {
				Set<byte[]> rolePairs = localStore.smembers(role);
				currentQueueSize = rolePairs.size();
				totalCount += currentQueueSize;
				if(max < currentQueueSize)
					max = currentQueueSize;
				for(byte[] rolePair : rolePairs)
					if(rolePair.length != 8)
						throw new Exception("Should of length 8, is " + rolePair.length);
			}
			System.out.println("Avg role result size: " + (totalCount/allRoles.size()));
			System.out.println("Max role result size: " + max);
		}
		finally {
			localStore.disconnect();
		}
	}

	public static void main(String[] args) throws Exception {
		DataStats dataStats = new DataStats();
		dataStats.printAvgResultSize();
//		dataStats.printRolePairs();
	}

}
