package knoelab.classification;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import knoelab.classification.controller.CommunicationHandler;
import knoelab.classification.init.AxiomDistributionType;
import knoelab.classification.misc.AxiomDB;
import knoelab.classification.misc.Constants;
import knoelab.classification.misc.HostInfo;
import knoelab.classification.misc.Util;
import knoelab.classification.pipeline.PipelineManager;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Response;
import redis.clients.jedis.Tuple;


/**
 * Implements completion rule 3 i.e. Type-2
 * axioms. They belong to the normal form A < 3r.B
 * If A \in S(X) then add (X,B) to R(r).
 * 
 * @author Raghava
 *
 */
public class Type2AxiomProcessor extends RolePairHandler {

	private Jedis resultStore;
	private PipelineManager localHostPReader;
	private PipelineManager resultNodePReader;
	private List<Response<Set<String>>> responseList;
	private List<Response<Set<Tuple>>> elementScoreList;
	private HostInfo resultNode;
	private Jedis scoreDB;
	private CommunicationHandler communicationHandler;
	private String machineName;
	private List<Jedis> type1Stores;
	private String channel;
	private Set<String> type1Hosts;
	private long count = 0;
	
	public Type2AxiomProcessor(int port) {		
		super(port);
		resultNode = propertyFileHandler.getResultNode();
		resultStore = new Jedis(resultNode.getHost(), resultNode.getPort(), 
				Constants.INFINITE_TIMEOUT);
		channel = localStore.get(propertyFileHandler.getChannelKey());
		scoreDB = new Jedis(localHostInfo.getHost(), localHostInfo.getPort(), 
				Constants.INFINITE_TIMEOUT);
		scoreDB.select(AxiomDB.SCORE_DB.getDBIndex());
		
		localHostPReader = new PipelineManager(Collections.singletonList(localHostInfo), 
						propertyFileHandler.getPipelineQueueSize());
		resultNodePReader = new PipelineManager(Collections.singletonList(resultNode), 
						propertyFileHandler.getPipelineQueueSize());
		type1Hosts = localStore.smembers(AxiomDistributionType.CR_TYPE1_1.toString());
		type1Hosts.addAll(localStore.smembers(
					AxiomDistributionType.CR_TYPE1_2.toString()));
		type1Stores = new ArrayList<Jedis>();
		for(String host : type1Hosts) {
			String[] hostPort = host.split(":");
			type1Stores.add(new Jedis(hostPort[0], 
					Integer.parseInt(hostPort[1]), Constants.INFINITE_TIMEOUT));
		}			
		String allChannelsKey = propertyFileHandler.getAllChannelsKey();
		communicationHandler = new CommunicationHandler(
				localStore.smembers(allChannelsKey));
		localStore.disconnect();
		try {
			machineName = InetAddress.getLocalHost().getHostName();
		}
		catch(Exception e) { e.printStackTrace(); }
	}

	public void processRules() throws Exception {
		Jedis localStore = new Jedis(localHostInfo.getHost(), localHostInfo.getPort(), 
									 Constants.INFINITE_TIMEOUT); 
		try {
			String localKeys = propertyFileHandler.getLocalKeys();
			Set<String> keySet = localStore.smembers(localKeys);
			List<String> keyLst = new ArrayList<String>(keySet);
			boolean continueProcessing = false;
			boolean nextIteration = true;
			boolean axiomUpdate;
			int iterationCount = 1;
			boolean firstIteration = true;
			Set<String> allKeys = localStore.smembers(localKeys);
			Util.setScore(scoreDB, Constants.KEYS_UPDATED, 0);
			// current keys from Type1 can be from multiple nodes
			for(String type1Host : type1Hosts)
				Util.setScore(scoreDB, Constants.CURRENT_KEYS, type1Host, 0);
			
		 	while(nextIteration) {
				if(!firstIteration) {
					// get keysUpdated from Type3_2
					Set<String> keysUpdated = new HashSet<String>(
							getKeysByScore(Constants.KEYS_UPDATED, true));
					// get currKeys from Type1
					// intersect with allKeys -- that is keySet
					keySet.clear();
					keyLst.clear();
					keySet = new HashSet<String>(
							getKeysByScore(Constants.CURRENT_KEYS, false));
					keySet.addAll(keysUpdated);
					keySet.retainAll(allKeys);
					keyLst.addAll(keySet);
					keySet.clear();
				}
				System.out.println("\nAxioms to process: " + keyLst.size());
				for(String axiomKey : keyLst) {
					localHostPReader.psmembers(localHostInfo, axiomKey, AxiomDB.NON_ROLE_DB);
					double score = Util.getScore(scoreDB, axiomKey);
					// Constants.SCORE_INCREMENT has been added to score in order to replicate 
					// the functionality of (score i.e. exclude 'score' in the zrange
					resultNodePReader.pZRangeByScore(resultNode, axiomKey, 
							score+Constants.SCORE_INCREMENT, Double.POSITIVE_INFINITY, 
							AxiomDB.NON_ROLE_DB);
				}
				localHostPReader.synchAll(AxiomDB.NON_ROLE_DB);
				responseList = localHostPReader.getSmembersResponseList();	
				resultNodePReader.synchAll(AxiomDB.NON_ROLE_DB);
				elementScoreList = resultNodePReader.getZrangeByScoreResponseList();
				
				int i = 0;
				for(Response<Set<String>> axiomVals : responseList) {
					axiomUpdate = applyRule(keyLst.get(i), axiomVals.get(), 
							elementScoreList.get(i).get());
					continueProcessing = continueProcessing || axiomUpdate;
					i++;
				}
				if(i != elementScoreList.size())
					throw new Exception("i should be same as list size -- i: " + 
							i + "  lst size: " + elementScoreList.size() + 
							"  responseLst size: " + responseList.size());
				resultNodePReader.resetSynchResponse();
				localHostPReader.resetSynchResponse();
//				keySet.clear();
				
				nextIteration = continueWithNextIteration(
						continueProcessing, iterationCount);
				System.out.println("nextIteration? " + nextIteration);
				System.out.println("count of writes to nimbus6 DB-1: " + count);
				iterationCount++;
				continueProcessing = false;
				firstIteration = false;
			}
		}
		finally {
			resultNodePReader.closeAll();
			localHostPReader.closeAll();
			localStore.disconnect();
			scoreDB.disconnect();
			resultStore.disconnect();
			for(Jedis type1Store : type1Stores)
				type1Store.disconnect();
			cleanUp();
		}
	}
	
	private Set<String> getKeysByScore(String key, boolean isResultStore) {
		Set<String> keys = new HashSet<String>();
		if(isResultStore) {
			double currentMinScore = Util.getScore(scoreDB, key);
			Set<Tuple> keysEScores = 
				resultStore.zrangeByScoreWithScores(
					key, currentMinScore+Constants.SCORE_INCREMENT, 
					Double.POSITIVE_INFINITY);
			for(Tuple tuple : keysEScores) {
				keys.add(tuple.getElement());
				if(tuple.getScore() > currentMinScore)
					currentMinScore = tuple.getScore();
			}
			Util.setScore(scoreDB, key, currentMinScore);
		}
		else {
			int i = 0;
			for(String type1Host : type1Hosts) {
				double currentMinScore = Util.getScore(scoreDB, key, type1Host);
				Set<Tuple> tuples = type1Stores.get(i).zrangeByScoreWithScores(
						key, currentMinScore+Constants.SCORE_INCREMENT, 
						Double.POSITIVE_INFINITY);
				for(Tuple tuple : tuples) {
					keys.add(tuple.getElement());
					if(tuple.getScore() > currentMinScore)
						currentMinScore = tuple.getScore();
				}
				Util.setScore(scoreDB, key, type1Host, currentMinScore);
				i++;
			}
		}	
		return keys;
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
	
	private boolean applyRule(String axiomKey, Set<String> axiomValue, 
			Set<Tuple> xvalues) throws Exception {		
		double nextMinScore = 0;
		numUpdates = new Long(0);
		Set<String> xvals = new HashSet<String>(xvalues.size());
		for(Tuple x : xvalues) {
			xvals.add(x.getElement());
			if(x.getScore() > nextMinScore)
				nextMinScore = x.getScore();
		}
		Util.setScore(scoreDB, axiomKey, nextMinScore);
		xvalues.clear();
		for(String ce : axiomValue) {
			// R(r) = R(r) U {(X,B)}  				
			// key: Br, value: X
			numUpdates += insertRolePair(ce, xvals);
			//TODO: move the eval stmt to insert keyList & valueList here
			//		Do this for every key rather than all keys -- unnecessary 
			//		passing of same keys over network 
		}
		numUpdates += kvInsertion(type32HostJedisMap, type32HostKVWrapperMap);
		numUpdates += kvInsertion(type4HostJedisMap, type4HostKVWrapperMap);
		type5PipelineManager1.synchAll(AxiomDB.ROLE_DB);
		type5PipelineManager4.synchAll(AxiomDB.DB4);
//		numUpdates += kvInsertion(type5HostJedisMap, type5HostKVWrapperMap);
		
		return (numUpdates > 0)?true:false;
	}
}

