package knoelab.classification;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
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
import redis.clients.jedis.JedisShardInfo;
import redis.clients.jedis.Response;
import redis.clients.jedis.ShardedJedis;
import redis.clients.jedis.Tuple;
import redis.clients.util.Hashing;

/**
 * Implements part-1 of completion rule 4 i.e. Type-3
 * axioms. It is split into 2 rules.
 * Type-3_1: if 3r.A < B and A \in S(Y) then
 * 			 3r.Y < B
 * 
 * @author Raghava
 *
 */
public class Type3_1AxiomProcessor {

	private PropertyFileHandler propertyFileHandler;
	private PipelineManager resultNodePReader;
	private HostInfo resultNode;
	private Jedis scoreDB;
	private HostInfo localHostInfo;
	private String checkAndInsertScript;
	private CommunicationHandler communicationHandler;
	private String machineName;
	private Jedis resultStore;
	private ShardedJedis type3_2ShardedJedis;
	private Map<String, Jedis> hostJedisMap;
	private Map<String, KeyValueWrapper> hostKVWrapperMap;
	private Set<String> type1Hosts;
	private List<Jedis> type1Stores;
	private String channel;
	
	public Type3_1AxiomProcessor(int localHostPort) throws Exception {
		propertyFileHandler = PropertyFileHandler.getInstance();
		localHostInfo = propertyFileHandler.getLocalHostInfo();
		localHostInfo.setPort(localHostPort);
		resultNode = propertyFileHandler.getResultNode();
		resultNodePReader = new PipelineManager(Collections.singletonList(resultNode), 
				propertyFileHandler.getPipelineQueueSize());
		scoreDB = new Jedis(localHostInfo.getHost(), 
					localHostInfo.getPort(), Constants.INFINITE_TIMEOUT);
		scoreDB.select(AxiomDB.SCORE_DB.getDBIndex());	
		
		Jedis localStore = new Jedis(localHostInfo.getHost(), 
				localHostInfo.getPort(), Constants.INFINITE_TIMEOUT);
		Map<AxiomDistributionType, List<HostInfo>> typeHostInfo = 
			propertyFileHandler.getTypeHostInfo();
		List<HostInfo> type32Hosts = typeHostInfo.get(
						AxiomDistributionType.CR_TYPE3_2);
		List<JedisShardInfo> type32Shards = new ArrayList<JedisShardInfo>();
		hostJedisMap = new HashMap<String, Jedis>();
		hostKVWrapperMap = new HashMap<String, KeyValueWrapper>();
		for(HostInfo hostInfo : type32Hosts) {
			type32Shards.add(new JedisShardInfo(hostInfo.getHost(), 
					hostInfo.getPort(), Constants.INFINITE_TIMEOUT));
			Jedis jedis = new Jedis(hostInfo.getHost(), hostInfo.getPort(), 
					Constants.INFINITE_TIMEOUT);
			// need to connect explicitly for lua scripting - gives a NPE if not done.
			jedis.connect();
			hostJedisMap.put(hostInfo.toString(), jedis);
			hostKVWrapperMap.put(hostInfo.toString(), new KeyValueWrapper());
		}
		type3_2ShardedJedis = new ShardedJedis(type32Shards, 
									Hashing.MURMUR_HASH);
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
		channel = localStore.get(propertyFileHandler.getChannelKey());
		checkAndInsertScript =
			  "local size = redis.call('ZCARD', 'localkeys') " +
			  "local localKeyScore = 1.0 " +
			  "local escore " +
			  "if(size == 0) then " +
			  		"localKeyScore = 1.0 " +
			  "else " +
			  		"escore = redis.call('ZRANGE', 'localkeys', -1, -1, 'WITHSCORES') " +
			  		"localKeyScore = escore[2] + " + Constants.SCORE_INCREMENT + " " +
			  "end " + 
			  "local prevKey = '' " +
			  "local currKey = '' " +
			  "local unique = 0 " + 
			  "size = #KEYS " +
			  "local score " +
			  "local ret " +
			  "for i=1,size do " +
			  		"if(not redis.call('ZSCORE', KEYS[i], ARGV[i])) then " +
			  			"if(redis.call('ZCARD', KEYS[i]) == 0) then " +
			  				"ret = redis.call('ZADD', KEYS[i], 1.0, ARGV[i]) " +
			  			"else " +
			  				"escore = redis.call('ZRANGE', KEYS[i], -1, -1, 'WITHSCORES') " +
			  				"score = escore[2] + " + Constants.SCORE_INCREMENT + " " +
			  				"ret = redis.call('ZADD', KEYS[i], score, ARGV[i]) " +
			  			"end " +
			  			"unique = unique + ret " +
			  			"currKey = KEYS[i] " +
			  			"if(currKey ~= prevKey) then " +
			  				"redis.call('ZADD', 'localkeys', localKeyScore, currKey) " +
			  				"prevKey = currKey " +
			  			"end " +
			  		"end " +
			  	"end " +
			  	"return unique ";
		
		String allChannelsKey = propertyFileHandler.getAllChannelsKey();
		communicationHandler = new CommunicationHandler(
				localStore.smembers(allChannelsKey));
		resultStore = new Jedis(resultNode.getHost(), 
						resultNode.getPort(), Constants.INFINITE_TIMEOUT);
		localStore.disconnect();
		try {
			machineName = InetAddress.getLocalHost().getHostName();
		}
		catch(Exception e) { e.printStackTrace(); }
	}
	
	public void processRules() throws Exception {
		Jedis localStore = new Jedis(localHostInfo.getHost(), 
				localHostInfo.getPort(), Constants.INFINITE_TIMEOUT);
		HostInfo localHostInfo = propertyFileHandler.getLocalHostInfo();
		PipelineManager localHostPReader = new PipelineManager(Collections.singletonList(localHostInfo), 
				propertyFileHandler.getPipelineQueueSize());
		List<Response<Set<String>>> smembersResponseList;
		List<Response<Set<Tuple>>>  zrangeResponseList;
		
		try {
			String localKeys = propertyFileHandler.getLocalKeys();
			Set<String> keySet = localStore.smembers(localKeys);
			
			boolean continueProcessing = false;
			boolean nextIteration = true;
			boolean axiomUpdate;
			int iterationCount = 1;
			List<String> keyList = new ArrayList<String>(keySet);
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
					keyList.clear();
					keySet = new HashSet<String>(
							getKeysByScore(Constants.CURRENT_KEYS, false));
					keySet.addAll(keysUpdated);
					keySet.retainAll(allKeys);
					keyList.addAll(keySet);
				}
				System.out.println("\nAxioms to process: " + keySet.size());
				for(String axiomKey : keySet) {
					localHostPReader.psmembers(localHostInfo, axiomKey, AxiomDB.NON_ROLE_DB);
					double score = Util.getScore(scoreDB, axiomKey);
					// SCORE_INCREMENT has been added to score in order to replicate the 
					// functionality of (score i.e. exclude 'score' in the zrange
					resultNodePReader.pZRangeByScore(resultNode, axiomKey, 
							score+Constants.SCORE_INCREMENT, Double.POSITIVE_INFINITY, 
							AxiomDB.NON_ROLE_DB);
				}
				localHostPReader.synchAll(AxiomDB.NON_ROLE_DB);
				resultNodePReader.synchAll(AxiomDB.NON_ROLE_DB);
				smembersResponseList = localHostPReader.getSmembersResponseList();	
				zrangeResponseList = resultNodePReader.getZrangeByScoreResponseList();
				
				int i = 0;				
				for(Response<Set<String>> smembersResponse : smembersResponseList) {
					axiomUpdate = applyRule(keyList.get(i), 
							smembersResponse.get(), zrangeResponseList.get(i).get());
					continueProcessing = continueProcessing || axiomUpdate;
					i++;
				}
				if(i != zrangeResponseList.size())
					throw new Exception("i should be same as list size");
				resultNodePReader.resetSynchResponse();
				localHostPReader.resetSynchResponse();
				nextIteration = continueWithNextIteration(
						continueProcessing, iterationCount);
				iterationCount++;
				System.out.println("nextIteration? " + nextIteration);
				continueProcessing = false;
				firstIteration = false;
			}
		}
		finally {
			resultNodePReader.closeAll();
			localHostPReader.closeAll();
			scoreDB.disconnect();
			localStore.disconnect();
			type3_2ShardedJedis.disconnect();
			for(Jedis type1Store : type1Stores)
				type1Store.disconnect();
			communicationHandler.disconnect();
			resultStore.disconnect();
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
	
	private boolean applyRule(String axiomKey, Set<String> superClassRoles, 
			Set<Tuple> yvalues) {
		double nextMinScore = 0;
		Long numUpdates = new Long(0);
		Set<String> hostKeys = hostJedisMap.keySet();
		
		for(String classRole : superClassRoles) {
			String[] superClassRole = classRole.split(propertyFileHandler.getComplexAxiomSeparator());
			for(Tuple yScore : yvalues) {
				// key: Yr, value: B
				
				StringBuilder key = new StringBuilder(yScore.getElement()).
										append(propertyFileHandler.getComplexAxiomSeparator()).
										append(superClassRole[1]);
				JedisShardInfo shardInfo = type3_2ShardedJedis.getShardInfo(key.toString());
				String hostKey = shardInfo.getHost() + ":" + shardInfo.getPort();
				KeyValueWrapper kvWrapper = hostKVWrapperMap.get(hostKey);
				kvWrapper.addToKeyValueList(key.toString(), superClassRole[0]);		
				if(yScore.getScore() > nextMinScore)
					nextMinScore = yScore.getScore();
			}
			for(String host : hostKeys) {
				KeyValueWrapper kvWrapper = hostKVWrapperMap.get(host);
				if(!kvWrapper.keyList.isEmpty())
					numUpdates += (Long) hostJedisMap.get(host).eval(checkAndInsertScript, 
							kvWrapper.keyList, 
							kvWrapper.valueList);
				kvWrapper.keyList.clear();
				kvWrapper.valueList.clear();
			}
		}
		
		Util.setScore(scoreDB, axiomKey, nextMinScore);
		return (numUpdates > 0)?true:false;		
	}
}
