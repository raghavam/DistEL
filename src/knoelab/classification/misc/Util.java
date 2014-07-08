package knoelab.classification.misc;

import java.util.ArrayList;
import java.util.GregorianCalendar;
import java.util.List;

import knoelab.classification.controller.CommunicationHandler;
import knoelab.classification.init.EntityType;

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
	
	public static long getElapsedTime(GregorianCalendar startTime) {
		GregorianCalendar endTime = new GregorianCalendar();		
		return endTime.getTimeInMillis() - startTime.getTimeInMillis();
//		double totalDiff = (endTime.getTimeInMillis() - startTime.getTimeInMillis())/1000;
//		long totalMins = (long)totalDiff/60;
//		double totalSecs = totalDiff - (totalMins * 60);
	}
	
	public static double getElapsedTimeSecs(long startTime) {
		long endTime = System.nanoTime();
		long diffTime = endTime - startTime;
		double diffTimeSecs = ((double)diffTime/1000000000);
		return diffTimeSecs;
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
	
	public static String getPackedID(long id, EntityType entityType) {
		String str = Long.toString(id);
		//2 places for length of ID and 1 for whether its a class/individual/role
		StringBuilder packedID = new StringBuilder(str.length() + 3);
		packedID.append(String.format("%02d", str.length()));
		packedID.append(str);
		packedID.append(entityType.getTypeID());
		return packedID.toString();
	}
	
	public static List<String> unpackIDs(String packedIDs) {
		List<String> unpackedIDs = new ArrayList<String>();
		int start = 0;
		while(start < packedIDs.length()) {
			//first two places go to the length of the ID
			int idLength = Integer.parseInt(packedIDs.substring(start, 
												start + 2));
			int totalLength = idLength + 3;
			unpackedIDs.add(packedIDs.substring(start, start + totalLength));
			start = start + totalLength;
		}
		return unpackedIDs;
	}

/*	
	public static boolean continueWithNextIteration(
			CommunicationHandler communicationHandler, String machineName, 
			String channel, boolean currIterStatus, int iterationCount) {
		StringBuilder message = new StringBuilder(machineName);
		message.append("~").append(iterationCount);
		// 0 - update; 1 - no update
		if(currIterStatus) {
			// there are some updates from at least one axiom
			message.append("~").append(0);
		}
		else {
			// there are no updates from any of the axioms
			message.append("~").append(1);
		}
		System.out.println("continueProcessing? " + currIterStatus);
		communicationHandler.broadcast(message.toString());
		System.out.println("Iteration " + iterationCount + " completed");
		return communicationHandler.removeAndGetStatus(
									channel, iterationCount);
	}
*/
	
	public static void broadcastMessage(
			CommunicationHandler communicationHandler, String machineName,
			boolean currIterStatus, int iterationCount) {
		StringBuilder message = new StringBuilder(machineName);
		message.append("~").append(iterationCount);
		// 0 - update; 1 - no update
		if(currIterStatus) {
			// there are some updates from at least one axiom
			message.append("~").append(0);
		}
		else {
			// there are no updates from any of the axioms
			message.append("~").append(1);
		}
		System.out.println("continueProcessing? " + currIterStatus);
		communicationHandler.broadcast(message.toString());
		System.out.println("Iteration " + iterationCount + " completed");
	}
}
