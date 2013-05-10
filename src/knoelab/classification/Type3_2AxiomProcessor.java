package knoelab.classification;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import knoelab.classification.controller.CommunicationHandler;
import knoelab.classification.misc.AxiomDB;
import knoelab.classification.misc.Constants;
import knoelab.classification.misc.HostInfo;
import knoelab.classification.misc.PropertyFileHandler;
import knoelab.classification.pipeline.PipelineManager;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;
import redis.clients.jedis.Transaction;
import redis.clients.jedis.Tuple;

/**
 * Implements part-2 of completion rule 4 i.e. Type-3
 * axioms. It is split into 2 rules.
 * Type-3_2: if 3r.Y < B and (X,Y) \in R(r) then
 * 			 S(X) = S(X) U {B} 
 * @author Raghava
 *
 */
public class Type3_2AxiomProcessor {

	private Jedis roleStore;
	private Jedis localStore;
	private HostInfo resultHostInfo;
	private PropertyFileHandler propertyFileHandler;
	private HostInfo localHostInfo;
	private Jedis resultStore;
	private Jedis scoreDB;
	private String insertionScript;
	private CommunicationHandler communicationHandler;
	private String machineName;
	private String channel;
	private int foundBottom;
	
	public Type3_2AxiomProcessor(int port) {
		propertyFileHandler = PropertyFileHandler.getInstance();
		localHostInfo = propertyFileHandler.getLocalHostInfo();
		localHostInfo.setPort(port);
		localStore = new Jedis(localHostInfo.getHost(), 
							port, Constants.INFINITE_TIMEOUT);
		roleStore = new Jedis(localHostInfo.getHost(), 
							localHostInfo.getPort(), Constants.INFINITE_TIMEOUT);
		roleStore.select(AxiomDB.ROLE_DB.getDBIndex());
		scoreDB = new Jedis(localHostInfo.getHost(), 
							localHostInfo.getPort(), Constants.INFINITE_TIMEOUT);
		scoreDB.select(AxiomDB.SCORE_DB.getDBIndex());
		resultHostInfo = propertyFileHandler.getResultNode();
		resultStore = new Jedis(resultHostInfo.getHost(), 
							resultHostInfo.getPort(), Constants.INFINITE_TIMEOUT);
		// need to connect explicitly for lua scripting - gives a NPE if not done.
		resultStore.connect();
		channel = localStore.get(propertyFileHandler.getChannelKey());
		foundBottom = Integer.parseInt(localStore.get("foundBottom"));
		
		insertionScript = "local unique = 0 " +
						  "local size = redis.call('ZCARD', 'keysUpdated') " +
						  "local keysUpdatedScore " +
						  "local highestEScore " +
						  "local escore " +
						  "local score " +
						  "local ret " +
						  "if(size == 0) then " +
						  	"keysUpdatedScore = 1.0 " +
						  "else " +
						  	"highestEScore = redis.call('ZRANGE', 'keysUpdated', " +
						  							"-1, -1, 'WITHSCORES') " +
						  	"keysUpdatedScore = highestEScore[2] + " + 
						  							Constants.SCORE_INCREMENT + " " +
						  "end " + 
						  "for index1,value1 in pairs(KEYS) do " +
	  						"escore = redis.call('ZRANGE', value1, -1, -1, 'WITHSCORES') " +
	  						"score = escore[2] + " + Constants.SCORE_INCREMENT + " " +
	  						"for index2,value2 in pairs(ARGV) do " +
	  							"if(not redis.call('ZSCORE', value1, value2)) then  " +
	  								"ret = redis.call('ZADD', value1, score, value2) " +
	  								"if(ret > 0) then " +
	  									"redis.call('ZADD', 'keysUpdated', " +
	  													"keysUpdatedScore, value1) " +
	  								"end " +
	  								"unique = unique + ret " +
	  							"end " +
	  						"end " +
	  					  "end " +
	  					  "return unique ";		
		
		String allChannelsKey = propertyFileHandler.getAllChannelsKey();
		communicationHandler = new CommunicationHandler(
				localStore.smembers(allChannelsKey));
		try {
			machineName = InetAddress.getLocalHost().getHostName();
		}
		catch(Exception e) { e.printStackTrace(); }
	}
	
	
	public void processRules() throws Exception {			
		PipelineManager localHostPReader = new PipelineManager(
				Collections.singletonList(localHostInfo), 
				propertyFileHandler.getPipelineQueueSize());
		List<Response<Set<Tuple>>> zrangeResponseList1;
		List<Response<Set<Tuple>>> zrangeResponseList2;
		Pipeline resultStorePipeline = resultStore.pipelined();
		List<Response<Double>> zscoreResponseList = new ArrayList<Response<Double>>();
		
		try {
			String localKeys = propertyFileHandler.getLocalKeys();
			
			boolean continueProcessing = false;
			boolean continue1 = false;
			boolean continue2 = false;
			boolean nextIteration = true;
			boolean axiomUpdate;
			int iterationCount = 1;
			
			double score1 = Constants.INIT_SCORE;
			double score2 = Constants.INIT_SCORE;
			double nextMinScore1;
			double nextMinScore2;
			boolean whichDB;
			if(foundBottom == 1)
				System.out.println("owl:Nothing found. So processing C5 rule.");
			
			while(nextIteration) {
				
				Transaction localTransaction = localStore.multi();
				Response<Set<Tuple>> keySetResponse1 = localTransaction.zrangeByScoreWithScores(
														localKeys, score1, Double.POSITIVE_INFINITY);
				Response<Set<String>> totalKeys1 = localTransaction.zrange(localKeys, 0, -1);
				localTransaction.exec();
				Transaction roleTransaction = roleStore.multi();
				Response<Set<Tuple>> keySetResponse2 = roleTransaction.zrangeByScoreWithScores(
														localKeys, score2, Double.POSITIVE_INFINITY);
				Response<Set<String>> totalKeys2 = roleTransaction.zrange(localKeys, 0, -1);
				roleTransaction.exec();
				
				if(keySetResponse1.get().isEmpty() && keySetResponse2.get().isEmpty()) {
					System.out.println("\nBoth key sets are empty, so skipping...");
					nextIteration = continueWithNextIteration(false, iterationCount);
					iterationCount++;
					System.out.println("nextIteration? " + nextIteration);
					continue1 = false;
					continue2 = false;				
					continue;
				}
				
				System.out.println("\nAxioms to process part-1: " + keySetResponse1.get().size() + 
						"  Total: " + totalKeys1.get().size());
				totalKeys1.get().clear();
				
				int i = 0;
				// PART-1
				if(!keySetResponse1.get().isEmpty()) {
					whichDB = true;
					for(Tuple axiomKey : keySetResponse1.get()) {
						nextMinScore1 = getScore(axiomKey.getElement(), whichDB);
						
						nextMinScore1 += Constants.SCORE_INCREMENT;
						// SCORE_INCREMENT has been added to score in order to replicate the 
						// functionality of (score i.e. exclude 'score' in the zrange	
						
						localHostPReader.pZRangeByScore(localHostInfo, axiomKey.getElement(), 
								nextMinScore1, Double.POSITIVE_INFINITY, AxiomDB.NON_ROLE_DB);
					}
					localHostPReader.synchAll(AxiomDB.NON_ROLE_DB);
					zrangeResponseList1 = localHostPReader.getZrangeByScoreResponseList();
					
					for(Tuple axiomKey : keySetResponse1.get()) {
						Set<Tuple> response1 = zrangeResponseList1.get(i).get();
						if(!response1.isEmpty()) {
							axiomUpdate = applyRule(axiomKey.getElement(), 
									response1, whichDB);
							continue1 = continue1 || axiomUpdate;
						}
						i++;
						if(axiomKey.getScore() > score1)
							score1 = axiomKey.getScore();
					}
					score1 += Constants.SCORE_INCREMENT;
					if(i != zrangeResponseList1.size())
						throw new Exception("i should be same as list size");
					localHostPReader.resetSynchResponse();
				}
				
				System.out.println("Axioms to process part-2: " + keySetResponse2.get().size() + 
						"  Total: " + totalKeys2.get().size());
				totalKeys2.get().clear();
				
				// PART-2
				if(!keySetResponse2.get().isEmpty()) {
					whichDB = false;
					for(Tuple axiomKey : keySetResponse2.get()) {
						nextMinScore2 = getScore(axiomKey.getElement(), whichDB);
						nextMinScore2 += Constants.SCORE_INCREMENT;
						
						// SCORE_INCREMENT has been added to score in order to replicate 
						// the functionality of (score i.e. exclude 'score' in the zrange
						
						localHostPReader.pZRangeByScore(localHostInfo, axiomKey.getElement(), 
								nextMinScore2, Double.POSITIVE_INFINITY, AxiomDB.ROLE_DB);
						
						if(foundBottom == 1)
							zscoreResponseList.add(resultStorePipeline.zscore(
									Long.toString(Constants.BOTTOM_ID), 
									axiomKey.getElement().split(
								propertyFileHandler.getComplexAxiomSeparator())[0]));
					}
					localHostPReader.synchAll(AxiomDB.ROLE_DB);
					zrangeResponseList2 = localHostPReader.getZrangeByScoreResponseList();
					if(foundBottom == 1) 
						resultStorePipeline.sync();
					i = 0;
					
					for(Tuple axiomKey : keySetResponse2.get()) {
						Set<Tuple> response2 = zrangeResponseList2.get(i).get();
						if(!response2.isEmpty()) {
							axiomUpdate = applyRule(axiomKey.getElement(), 
									response2, whichDB);
							continue2 = continue2 || axiomUpdate;
							
							if(foundBottom == 1) {
								if(zscoreResponseList.get(i).get() != null) {
									// add response2 to bottom on resultStore
									for(Tuple t : response2)
										resultStorePipeline.zadd(
											Long.toString(Constants.BOTTOM_ID), 
											Constants.INIT_SCORE, t.getElement());
								}
							}
						}
						i++;
						if(axiomKey.getScore() > score2)
							score2 = axiomKey.getScore();
					}
					score2 += Constants.SCORE_INCREMENT;
					if(i != zrangeResponseList2.size())
						throw new Exception("i should be same as list size");
					localHostPReader.resetSynchResponse();
					if(foundBottom == 1)
						resultStorePipeline.sync();
				}
				keySetResponse1.get().clear();
				keySetResponse2.get().clear();
				
				if(foundBottom == 1) {
					//TODO: this if block is not tested
					/*TODO: 2nd case of change in bottom on resultStore is 
					 * yet to be done. At the beginning of next iteration check 
					 * whether there are any changes to scores in bottom. If there 
					 * are, then get the new changes and check whether these new 
					 * changes are in an prev. keys of type3_2. For the new keys of 
					 * type3_2, do as before i.e., check whether these keys are in 
					 * bottom.if there are any changes from these (additions to bottom) 
					 * then change the nextIteration flag accordingly.
					 */
				}
									
				continueProcessing = continue1 || continue2;				
				nextIteration = continueWithNextIteration(
						continueProcessing, iterationCount);
				iterationCount++;
				System.out.println("nextIteration? " + nextIteration);
				continue1 = false;
				continue2 = false;
			}
		}
		finally {
			localStore.disconnect();
			roleStore.disconnect();
			resultStore.disconnect();
			scoreDB.disconnect();
			localHostPReader.closeAll();
			communicationHandler.disconnect();
		}
	}
	
	private boolean continueWithNextIteration(boolean currIterStatus, 
			int iterationCount) {
		String message = machineName + "~" + iterationCount;
		// 0 - update; 1 - no update
		if(currIterStatus) {
			// there are some updates from at least one axiom
			message = message + "~" + 0;
		}
		else {
			// there are no updates from any of the axioms
			message = message + "~" + 1;
		}
		System.out.println("continueProcessing? " + currIterStatus);
		communicationHandler.broadcast(message);
		System.out.println("Iteration " + iterationCount + " completed");
		return communicationHandler.removeAndGetStatus(
									channel, iterationCount);
	}

	/**
	 * 
	 * @param axiomKey
	 * @param axiomValue
	 * @param whichDB true represents DB-0 and false represents DB-1 
	 */
	private boolean applyRule(String axiomKey, Set<Tuple> axiomValueScore, 
			boolean whichDB) throws Exception {
		Set<String> rolePairs = null;
		if(whichDB)
			rolePairs = roleStore.zrange(axiomKey, 0, -1);
		else
			rolePairs = localStore.zrange(axiomKey, 0, -1);	
		if(rolePairs.isEmpty())
			return false;
		
		List<String> axiomValue = new ArrayList<String>(axiomValueScore.size());
		double score = 0;
		for(Tuple t : axiomValueScore) {
			axiomValue.add(t.getElement());
			if(t.getScore() > score)
				score = t.getScore();
		}
		
		Long numUpdates = new Long(0);
		if(whichDB) {		
			if(!axiomValue.isEmpty() && !rolePairs.isEmpty()) {
				numUpdates = (Long)resultStore.eval(insertionScript, axiomValue, 
						new ArrayList<String>(rolePairs));
			}
		}
		else {
			if(!axiomValue.isEmpty() && !rolePairs.isEmpty()) {
				numUpdates = (Long)resultStore.eval(insertionScript, new ArrayList<String>(rolePairs), 
					axiomValue);
			}
		}
		setScore(axiomKey, score, whichDB);
//		System.out.println("NumUpdates: " + numUpdates);
		return (numUpdates > 0)?true:false;
	}	
	
	private double getScore(String axiomKey, boolean whichDB) {
		String score;
		if(whichDB) 
			score = scoreDB.hget(axiomKey, Constants.FIELD1);
		else
			score = scoreDB.hget(axiomKey, Constants.FIELD2);
		if(score == null)
			score = "0";		
		return Double.parseDouble(score);
	}
	
	private void setScore(String axiomKey, double score, boolean whichDB) {
		if(whichDB)
			scoreDB.hset(axiomKey, Constants.FIELD1, Double.toString(score));
		else
			scoreDB.hset(axiomKey, Constants.FIELD2, Double.toString(score));
	}
}
