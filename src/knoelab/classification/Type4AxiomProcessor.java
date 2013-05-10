package knoelab.classification;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import knoelab.classification.controller.CommunicationHandler;
import knoelab.classification.misc.AxiomDB;
import knoelab.classification.misc.Constants;
import knoelab.classification.misc.Util;
import knoelab.classification.pipeline.PipelineManager;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Response;
import redis.clients.jedis.Tuple;


/**
 * Implements completion rule 4 i.e. Type-4
 * axioms. They belong to the normal forms r < s.
 * 
 * @author Raghava
 *
 */
public class Type4AxiomProcessor extends RolePairHandler {

	private String localKeys;
	private CommunicationHandler communicationHandler;
	private String machineName;
	private String channel;
	private Jedis scoreDB;
	
	public Type4AxiomProcessor(int localHostPort) {
		super(localHostPort);
		localKeys = propertyFileHandler.getLocalKeys();
		scoreDB = new Jedis(localHostInfo.getHost(), 
				localHostInfo.getPort(), Constants.INFINITE_TIMEOUT);
		scoreDB.select(2);
		channel = localStore.get(propertyFileHandler.getChannelKey());		
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
		List<Response<Set<Tuple>>> zrangeResponseList;
		double score = Constants.INIT_SCORE;
		double nextMinScore;
		int iterationCount = 1;
		AxiomDB axiomDB = AxiomDB.ROLE_DB;
		
		try {		
			boolean continueProcessing = false;
			boolean nextIteration = true;
			boolean axiomUpdate;
			localStore.select(1);
			
			while(nextIteration) {
				Set<Tuple> keyScoreSet = localStore.zrangeByScoreWithScores(
								localKeys, score, Double.POSITIVE_INFINITY);
				List<Tuple> keyScoreList = new ArrayList<Tuple>(keyScoreSet);
				keyScoreSet.clear();
				System.out.println("\nAxioms to process: " + 
						keyScoreList.size());
				if(keyScoreList.isEmpty())
					continueProcessing = false;				
				else {
					for(Tuple keyScore : keyScoreList) {
						nextMinScore = Util.getScore(scoreDB, 
								keyScore.getElement());
						nextMinScore += Constants.SCORE_INCREMENT;
						// SCORE_INCREMENT has been added to score in order to replicate the 
						// functionality of (score i.e. exclude 'score' in the zrange	
						localHostPReader.pZRangeByScore(localHostInfo, 
								keyScore.getElement(), nextMinScore, 
								Double.POSITIVE_INFINITY, axiomDB);
						if(keyScore.getScore() > score)
							score = keyScore.getScore();
					}
					score += Constants.SCORE_INCREMENT;
					localHostPReader.synchAll(axiomDB);
					zrangeResponseList = 
							localHostPReader.getZrangeByScoreResponseList();
					int i = 0;
					for(Response<Set<Tuple>> response : zrangeResponseList) {
						axiomUpdate = applyRule(keyScoreList.get(i).getElement(), 
										response.get());
						continueProcessing = continueProcessing || axiomUpdate;
						i++;
					}
					keyScoreList.clear();
					localHostPReader.resetSynchResponse();
				}
				nextIteration = continueWithNextIteration(
						continueProcessing, iterationCount);
				iterationCount++;
				System.out.println("nextIteration? " + nextIteration);
				continueProcessing = false;
			}
		}
		finally {
			localHostPReader.closeAll();
			communicationHandler.disconnect();
			scoreDB.disconnect();
			cleanUp();
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

	private boolean applyRule(String axiomKey, Set<Tuple> xvalTuples) 
		throws Exception {
		numUpdates = new Long(0);
		String[] conceptRole = axiomKey.split(
				propertyFileHandler.getComplexAxiomSeparator());
		Set<String> superRoles = subRoleCache.get(conceptRole[1]);
		if(superRoles == null)
			superRoles = type4ShardedJedis.smembers(conceptRole[1]);
		if(superRoles == null)
			throw new Exception("r in r < s now found: " + 
					conceptRole[1]);
		Set<String> xvals = new HashSet<String>(xvalTuples.size());
		double nextMinScore = 0;
		for(Tuple t : xvalTuples) {
			xvals.add(t.getElement());
			if(t.getScore() > nextMinScore)
				nextMinScore = t.getScore();
		}
		xvalTuples.clear();
		Util.setScore(scoreDB, axiomKey, nextMinScore);
		for(String s : superRoles) {
			//form Ys
			StringBuilder conceptSuperRole = 
				new StringBuilder(conceptRole[0]).
				append(propertyFileHandler.getComplexAxiomSeparator()).
				append(s);
			numUpdates += insertRolePair(conceptSuperRole.toString(), xvals);
		}
		xvals.clear();
		numUpdates += kvInsertion(type32HostJedisMap, type32HostKVWrapperMap);
		numUpdates += kvInsertion(type4HostJedisMap, type4HostKVWrapperMap);
		type5PipelineManager1.synchAll(AxiomDB.ROLE_DB);
		type5PipelineManager4.synchAll(AxiomDB.DB4);
//		numUpdates += kvInsertion(type5HostJedisMap, type5HostKVWrapperMap);
		return (numUpdates > 0)?true:false;
	}
}
