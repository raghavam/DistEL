package knoelab.classification;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import knoelab.classification.controller.CommunicationHandler;
import knoelab.classification.init.AxiomDistributionType;
import knoelab.classification.misc.AxiomDB;
import knoelab.classification.misc.Constants;
import knoelab.classification.misc.HostInfo;
import knoelab.classification.misc.PropertyFileHandler;
import knoelab.classification.misc.Util;
import knoelab.classification.pipeline.PipelineManager;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Response;
import redis.clients.jedis.Tuple;

/**
 * Implements completion rule 2 i.e. axioms of type A1 ^ .... ^ An < B.
 * 
 * @author Raghava
 *
 */
public class Type1_2AxiomProcessor {

	private HostInfo resultHostInfo;
	private Jedis resultStore;
	private Jedis scoreDB;
	private PropertyFileHandler propertyFileHandler;
	private PipelineManager pipelinedReader;
	private HostInfo localHostInfo;
	private List<Response<Set<String>>> responseList;
	private String scriptNConjuncts;
	private Jedis localStore;
	private String machineName;
	private CommunicationHandler communicationHandler;	
	private String scriptCurrentKeysAdd;
	private List<String> keysToAdd;
	private String channel;
	private Set<String> type1Hosts;
	private List<Jedis> type1Stores;
	
	public Type1_2AxiomProcessor(int port) {
		propertyFileHandler = PropertyFileHandler.getInstance();
		resultHostInfo = propertyFileHandler.getResultNode();
		resultStore = new Jedis(resultHostInfo.getHost(), resultHostInfo.getPort(), 
							Constants.INFINITE_TIMEOUT);
		// need to connect explicitly for lua scripting - gives a NPE if not done.
		resultStore.connect();
		localHostInfo = propertyFileHandler.getLocalHostInfo();
		localHostInfo.setPort(port);
		int queueSize = propertyFileHandler.getPipelineQueueSize();
		pipelinedReader = new PipelineManager(Collections.singletonList(localHostInfo), queueSize);
		scoreDB = new Jedis(localHostInfo.getHost(), port, 
							Constants.INFINITE_TIMEOUT);
		scoreDB.select(AxiomDB.SCORE_DB.getDBIndex());
		localStore = new Jedis(localHostInfo.getHost(), port, 
							Constants.INFINITE_TIMEOUT);
		localStore.connect();
		channel = localStore.get(propertyFileHandler.getChannelKey());
		keysToAdd = new ArrayList<String>();
		type1Hosts = localStore.smembers(
							AxiomDistributionType.CR_TYPE1_1.toString());
		type1Hosts.addAll(localStore.smembers(
					AxiomDistributionType.CR_TYPE1_2.toString()));
		type1Stores = new ArrayList<Jedis>();
		for(String s : type1Hosts) {
			String[] hostPort = s.split(":");
			type1Stores.add(new Jedis(hostPort[0], 
					Integer.parseInt(hostPort[1]), Constants.INFINITE_TIMEOUT));
		}

		scriptNConjuncts = 
						   "local numKeys = #KEYS " +
						   "local unique = 0 " +
						   "redis.call('ZINTERSTORE', 'dummyKey123', " +
						   			"numKeys, unpack(KEYS)) " +
						   "local toBeAddedList = redis.call(" +
						   			"'ZRANGE', 'dummyKey123', 0, -1) " +
						   "local escore " +
						   "local score " +
						   "local ret " +
						   "for i1,v1 in pairs(ARGV) do " +
						   		"escore = redis.call('ZRANGE', v1, -1, -1, 'WITHSCORES') " +
						   		"score = escore[2] + " + Constants.SCORE_INCREMENT + " " +
						   		"for i2,v2 in pairs(toBeAddedList) do " +
						   			"if(not redis.call('ZSCORE', v1, v2)) then " +
						   				"ret = redis.call('ZADD', v1, score, v2) " +
						   				"unique = unique + ret " +
						   			"end " +
						   		"end " +
						   	"end " +
						   	"redis.call('DEL', 'dummyKey123') " +
						   	"return unique ";
		
		scriptCurrentKeysAdd = "local size = redis.call('ZCARD', KEYS[1]) " +
							   "local score " +
							   "local highestEScore " +
		  					   "if(size == 0) then " +
		  					   		"score = 1.0 " +
		  					   	"else " +
		  					   		"highestEScore = redis.call('ZRANGE', KEYS[1], " +
		  					   				"-1, -1, 'WITHSCORES') " +
		  					   		"score = highestEScore[2] + " + 
		  					   			Constants.SCORE_INCREMENT + " " +
		  						"end " +
		  						"for index,value in pairs(ARGV) do " +
		  							"redis.call('ZADD', KEYS[1], score, value) " +
		  						"end ";
		
		String allChannelsKey = propertyFileHandler.getAllChannelsKey();
		communicationHandler = new CommunicationHandler(
				localStore.smembers(allChannelsKey));
		try {
			machineName = InetAddress.getLocalHost().getHostName();
		}
		catch(Exception e) { e.printStackTrace(); }
	}
	
	public void processRules() throws Exception {		
		PipelineManager pipelineSetReader = new PipelineManager(
				Collections.singletonList(localHostInfo),
				propertyFileHandler.getPipelineQueueSize());
		try {
				boolean continueProcessing = false;
				boolean nextIteration = true;
				boolean axiomUpdate;
				int iterationCount = 1;
				String localKeys = propertyFileHandler.getLocalKeys();
//				localStore.sunionstore(Constants.CURRENT_KEYS, localKeys);
//				Set<String> allKeys = localStore.smembers(localKeys);
				localStore.select(AxiomDB.CONJUNCT_INDEX_DB.getDBIndex());
				Set<String> conjunctIndices = localStore.keys("*");
				localStore.select(0);
				Util.setScore(scoreDB, Constants.KEYS_UPDATED, 0);
				for(String type1Host : type1Hosts)
					Util.setScore(scoreDB, Constants.CURRENT_KEYS, type1Host, 0);
				boolean isFirstIteration = true;
				Set<String> keySet;
				
				while(nextIteration) {
					if(isFirstIteration) {
						keySet = localStore.smembers(localKeys);
						isFirstIteration = false;
					}
					else {
						// get the keys updated by Type3_2
						Set<String> keysUpdated = new HashSet<String>(
								getKeysByScore(Constants.KEYS_UPDATED, false));
						
						// add CURRENT_KEYS of all Type1 hosts
						Set<String> totalKeysUpdated = new HashSet<String>(
								getKeysByScore(Constants.CURRENT_KEYS, true));
						// add them to the current keys to be processed
						totalKeysUpdated.addAll(keysUpdated);
						
						keySet = new HashSet<String>();
						Set<String> commonConjuncts = new HashSet<String>(conjunctIndices);
						commonConjuncts.retainAll(totalKeysUpdated);
						for(String conjunctIndex : commonConjuncts)
							pipelineSetReader.psmembers(localHostInfo, 
								conjunctIndex, AxiomDB.CONJUNCT_INDEX_DB);
						pipelineSetReader.synchAll(AxiomDB.CONJUNCT_INDEX_DB);
						List<Response<Set<String>>> conjunctMembers = 
							pipelineSetReader.getSmembersResponseList();
						for(Response<Set<String>> members : conjunctMembers)
							keySet.addAll(members.get());
						pipelineSetReader.resetSynchResponse();
						commonConjuncts.clear();
					}
					System.out.println("Axioms to process: " + keySet.size());
					for(String axiomKey : keySet) 
						pipelinedReader.psmembers(localHostInfo, axiomKey, 
								AxiomDB.NON_ROLE_DB);
					pipelinedReader.synchAll(AxiomDB.NON_ROLE_DB);
					responseList = new ArrayList<Response<Set<String>>>(
							pipelinedReader.getSmembersResponseList());
					pipelinedReader.resetSynchResponse();
					int i = 0;
					//TODO: use threading as in 2-way joins. Don't wait until everything is
					//		read(pipelinedReader)
					for(String axiomKey : keySet) {
						if(responseList.get(i).get().isEmpty())
							axiomUpdate = false;
						else
							axiomUpdate = applyRule(axiomKey, responseList.get(i).get());
						continueProcessing = continueProcessing || axiomUpdate;
						i++;
					}
					System.out.println("Keys Updated: " + keysToAdd.size());
					if(!keysToAdd.isEmpty()) {
						if(keysToAdd.size() < Constants.SCRIPT_KEY_LIMIT)
							localStore.eval(scriptCurrentKeysAdd, 
									Collections.singletonList(
											Constants.CURRENT_KEYS), keysToAdd);
						else {
							int j = 1;
							int startIndex = 0;
							while(j <= keysToAdd.size()) {
								if((j%Constants.SCRIPT_KEY_LIMIT == 0) || 
										(j == keysToAdd.size())) {
									localStore.eval(scriptCurrentKeysAdd, 
											Collections.singletonList(
													Constants.CURRENT_KEYS), 
											keysToAdd.subList(startIndex, j));
									startIndex = j;
								}
								j++;
							}
						}
						keysToAdd.clear();
					}
					nextIteration = continueWithNextIteration(
							continueProcessing, iterationCount);
					System.out.println("nextIteration? " + nextIteration + "\n");
					iterationCount++;
					continueProcessing = false;
					keySet.clear();
				}
		}
		finally {
			localStore.disconnect();
			resultStore.disconnect();
			scoreDB.disconnect();
			pipelinedReader.closeAll();
			pipelineSetReader.closeAll();
			communicationHandler.disconnect();
			for(Jedis type1Store : type1Stores)
				type1Store.disconnect();
		}
	}
	
	private Set<String> getKeysByScore(String key, boolean isLocal) {
//		double currentMinScore = Util.getScore(scoreDB, key);
		Set<Tuple> keysEScores;
		Set<String> keys = new HashSet<String>();
		if(isLocal) {
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
		else {
			double currentMinScore = Util.getScore(scoreDB, key);
			keysEScores = 
				resultStore.zrangeByScoreWithScores(
					key, 
					currentMinScore+Constants.SCORE_INCREMENT, 
					Double.POSITIVE_INFINITY);
			for(Tuple tuple : keysEScores) {
				keys.add(tuple.getElement());
			if(tuple.getScore() > currentMinScore)
				currentMinScore = tuple.getScore();
			}
			Util.setScore(scoreDB, key, currentMinScore);
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
	
	private boolean applyRule(String axiomKey, Set<String> axiomValue) 
		throws Exception {
		String[] conjuncts = axiomKey.split(
				propertyFileHandler.getComplexAxiomSeparator());
		// there are n conjuncts on the LHS of the axiom 
		Long numUpdates = (Long) resultStore.eval(scriptNConjuncts, 
				Arrays.asList(conjuncts), new ArrayList<String>(axiomValue));
		if(numUpdates > 0) {
			for(String superClass : axiomValue)
				keysToAdd.add(superClass);
		}
		return (numUpdates > 0)?true:false;
	}
}
