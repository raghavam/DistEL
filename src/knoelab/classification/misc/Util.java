package knoelab.classification.misc;

import java.util.GregorianCalendar;

import redis.clients.jedis.Jedis;

/**
 * This class contains all the 
 * miscellaneous utility methods
 * 
 * @author Raghava
 *
 */
public class Util {
	
	/**
	 * Takes compound fragments such as class expressions (rA)
	 * or role chain (rs) and gives out individual fragments
	 * 
	 * @param compoundFragment
	 * @return
	 */
	public static byte[][] extractFragments(byte[] compoundFragment) {
		byte[][] fragments = new byte[2][Constants.NUM_BYTES];
		for(int i=0; i<Constants.NUM_BYTES; i++)
			fragments[0][i] = compoundFragment[i];
		for(int i=0; i<Constants.NUM_BYTES; i++)
			fragments[1][i] = compoundFragment[i+Constants.NUM_BYTES];
			
		return fragments;
	}
	
	public static String idToConcept(String conceptID, Jedis idReader) throws Exception {
		String concept = idReader.get(conceptID);
		if(concept == null)
			throw new Exception("Given concept ID does " +
					"not exist in DB: " + conceptID);
		return concept;
	}
	
	public static String conceptToID(String concept, Jedis idReader) throws Exception {
		String conceptID = idReader.get(concept);
		if(conceptID == null)
			throw new Exception("Concept does not exist in DB: " + concept);
		return conceptID;
	}	
/*	
	public static List<HostInfo> getTargetHostInfoList(AxiomDistributionType... axiomTypes) {
		List<HostInfo> targetHostInfo = new ArrayList<HostInfo>();	
		Jedis localStore = new Jedis(localHostInfo.getHost(), localHostInfo.getPort());
		for(AxiomDistributionType axiomType : axiomTypes) {
			String hostPort[] = localStore.hget("TypeHost", axiomType.toString()).trim().split(":");
			HostInfo targetHost = new HostInfo(hostPort[0], Integer.parseInt(hostPort[1]));
			targetHostInfo.add(targetHost);
		}
		localStore.disconnect();
		return targetHostInfo;
	}
*/	
	public static void printElapsedTime(GregorianCalendar startTime) {
		GregorianCalendar iterEnd = new GregorianCalendar();		
		double totalDiff = (iterEnd.getTimeInMillis() - startTime.getTimeInMillis())/1000;
		System.out.println("In secs: " + totalDiff);
		long totalMins = (long)totalDiff/60;
		double totalSecs = totalDiff - (totalMins * 60);
		System.out.println(totalMins + " mins and " + totalSecs + " secs");
	}
	
	public static double getScore(Jedis scoreDB, String axiomKey) {
		String score = scoreDB.get(axiomKey);
		if(score == null)
			score = "0";	// initialize score to 0
		return Double.parseDouble(score);
	}
	
	public static double getScore(Jedis scoreDB, String axiomKey, String field) {
		String score = scoreDB.hget(axiomKey, field);
		if(score == null)
			score = "0";	// initialize score to 0
		return Double.parseDouble(score);
	}
	
	public static void setScore(Jedis scoreDB, String axiomKey, double score) {
		scoreDB.set(axiomKey, Double.toString(score));
	}
	
	public static void setSize(Jedis sizeDB, String axiomKey, int size) {
		sizeDB.set(axiomKey, Integer.toString(size));
	}
	
	public static void setScore(Jedis scoreDB, String axiomKey, 
			String field, double score) {
		scoreDB.hset(axiomKey, field, Double.toString(score));
	}
}
