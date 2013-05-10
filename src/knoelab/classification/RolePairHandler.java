package knoelab.classification;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import knoelab.classification.init.AxiomDistributionType;
import knoelab.classification.misc.AxiomDB;
import knoelab.classification.misc.Constants;
import knoelab.classification.misc.HostInfo;
import knoelab.classification.misc.LRUCache;
import knoelab.classification.misc.PropertyFileHandler;
import knoelab.classification.pipeline.PipelineManager;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisShardInfo;
import redis.clients.jedis.ShardedJedis;
import redis.clients.util.Hashing;

/**
 * This class handles all the new (Yr,X) pair related to roles
 *  
 * @author Raghava
 *
 */
public class RolePairHandler {
	protected PropertyFileHandler propertyFileHandler;
	protected Jedis localStore;
	protected HostInfo localHostInfo;
	protected String localKeys;
	protected Map<String, Jedis> type32HostJedisMap;
	private ShardedJedis type32ShardedJedis;
	protected Map<String, Map<Integer,KeyValueWrapper>> type32HostKVWrapperMap;
	
	protected Map<String, Jedis> type4HostJedisMap;
	protected ShardedJedis type4ShardedJedis;
	protected Map<String, Map<Integer,KeyValueWrapper>> type4HostKVWrapperMap;
	
	protected Map<String, Jedis> type5HostJedisMap;
	protected ShardedJedis type5ShardedJedis;
	protected Map<String, Map<Integer,KeyValueWrapper>> type5HostKVWrapperMap;
	protected PipelineManager type5PipelineManager1;
	protected PipelineManager type5PipelineManager4;
	
	private final int DB1 = 1;
	private final int DB2 = 2;
	private final int DB3 = 3;
	private final int DB4 = 4;
	
	protected LRUCache<String, Set<String>> subRoleCache; // holds 'r' in r < s
	// holds 'r,st' in r o s < t
	protected LRUCache<String, Set<String>> roleChainLHS1Cache; 
	// holds 's,rt' in r o s < t
	protected LRUCache<String, Set<String>> roleChainLHS2Cache;
	protected String checkAndInsertScript;
	protected String insertionScript2;
	protected Long numUpdates;
	
	public RolePairHandler(int localHostPort) {
		propertyFileHandler = PropertyFileHandler.getInstance();
		localKeys = propertyFileHandler.getLocalKeys();
		localHostInfo = propertyFileHandler.getLocalHostInfo();
		localHostInfo.setPort(localHostPort);
		localStore = new Jedis(localHostInfo.getHost(), 
				localHostInfo.getPort(), Constants.INFINITE_TIMEOUT);
		subRoleCache = new LRUCache<String, Set<String>>(1000);
		roleChainLHS1Cache = new LRUCache<String, Set<String>>(100);
		roleChainLHS2Cache = new LRUCache<String, Set<String>>(100);
		numUpdates = new Long(0);
		initType32Shards();
		initType4Shards();
		initType5Shards();
		
		checkAndInsertScript =
			  "local size = redis.call('ZCARD', 'localkeys') " +
			  "local localKeyScore " +
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
			  "local esize " + 
			  "size = #KEYS " +
			  "local ret " +
			  "local score " +
			  "for i=1,size do " +
			  		"if(not redis.call('ZSCORE', KEYS[i], ARGV[i])) then " +
			  			"escore = redis.call('ZRANGE', KEYS[i], -1, -1, 'WITHSCORES') " +
			  			"esize = #escore " +
			  			"if(esize == 0) then " +
			  				"score = 1.0 " +
			  			"else " +
			  				"score = escore[2] + " + Constants.SCORE_INCREMENT + " " +
						"end " +
						"ret = redis.call('ZADD', KEYS[i], score, ARGV[i]) " +
			  			"unique = unique + ret " +
			  			"currKey = KEYS[i] " +
			  			"if(currKey ~= prevKey) then " +
			  				"redis.call('ZADD', 'localkeys', localKeyScore, currKey) " +
			  				"prevKey = currKey " +
			  			"end " +
			  		"end " +
			  	"end " +
			  	"return unique ";

		// remove the localkeys insertion and sadd() is sufficient
		insertionScript2 = 
			"local unique = 0 " + 
			"local size = #KEYS " +
			"local ret " +
			"local currKey " +
			"for i=1,size do " +
				"ret = redis.call('SADD', KEYS[i], ARGV[i]) " +
				"unique = unique + ret " +
				"currKey = KEYS[i] " +
			"end " +
			"return unique ";
	}
	
	private void initType32Shards() {
		Map<AxiomDistributionType, List<HostInfo>> typeHostInfo = 
			propertyFileHandler.getTypeHostInfo();
		List<HostInfo> type32Hosts = typeHostInfo.get(
						AxiomDistributionType.CR_TYPE3_2);
		List<JedisShardInfo> type32Shards = new ArrayList<JedisShardInfo>();
		type32HostKVWrapperMap = 
			new HashMap<String, Map<Integer,KeyValueWrapper>>();
		type32HostJedisMap = new HashMap<String, Jedis>();
		for(HostInfo hostInfo : type32Hosts) {	
			type32Shards.add(new JedisShardInfo(hostInfo.getHost(), 
					hostInfo.getPort(), Constants.INFINITE_TIMEOUT));
			Jedis jedis = new Jedis(hostInfo.getHost(), 
					hostInfo.getPort(), Constants.INFINITE_TIMEOUT);
			// need to connect explicitly for lua scripting - gives a NPE if not done.
			jedis.connect();
			type32HostJedisMap.put(hostInfo.toString(), jedis);
			type32HostKVWrapperMap.put(hostInfo.toString(), 
					new HashMap<Integer, KeyValueWrapper>());
		}
		type32ShardedJedis = new ShardedJedis(type32Shards, 
								Hashing.MURMUR_HASH);
	}
	
	private void initType4Shards() {
		Map<AxiomDistributionType, List<HostInfo>> typeHostInfo = 
			propertyFileHandler.getTypeHostInfo();
		List<HostInfo> type4Hosts = typeHostInfo.get(
						AxiomDistributionType.CR_TYPE4);
		List<JedisShardInfo> type4Shards = new ArrayList<JedisShardInfo>();
		type4HostKVWrapperMap = 
			new HashMap<String, Map<Integer,KeyValueWrapper>>();
		type4HostJedisMap = new HashMap<String, Jedis>();
		for(HostInfo hostInfo : type4Hosts) {
			type4Shards.add(new JedisShardInfo(hostInfo.getHost(), 
					hostInfo.getPort(), Constants.INFINITE_TIMEOUT));
			Jedis jedis = new Jedis(hostInfo.getHost(), 
					hostInfo.getPort(), Constants.INFINITE_TIMEOUT);
			// need to connect explicitly for lua scripting - gives a NPE if not done.
			jedis.connect();
			type4HostJedisMap.put(hostInfo.toString(), jedis);
			type4HostKVWrapperMap.put(hostInfo.toString(), 
					new HashMap<Integer, KeyValueWrapper>());
		}
		type4ShardedJedis = new ShardedJedis(type4Shards, 
								Hashing.MURMUR_HASH);
	}
	
	private void initType5Shards() {
		List<JedisShardInfo> type5Shards = new ArrayList<JedisShardInfo>();
		type5HostKVWrapperMap = 
			new HashMap<String, Map<Integer,KeyValueWrapper>>();
		type5HostJedisMap = new HashMap<String, Jedis>();
		Map<AxiomDistributionType, List<HostInfo>> typeHostInfo = 
					propertyFileHandler.getTypeHostInfo();
		List<HostInfo> type5Hosts = typeHostInfo.get(AxiomDistributionType.CR_TYPE5);
		for(HostInfo hostInfo : type5Hosts) {
			type5Shards.add(new JedisShardInfo(hostInfo.getHost(), 
					hostInfo.getPort(), Constants.INFINITE_TIMEOUT));
			Jedis jedis = new Jedis(hostInfo.getHost(), 
					hostInfo.getPort(), Constants.INFINITE_TIMEOUT);
			// need to connect explicitly for lua scripting - gives a NPE if not done.
			jedis.connect();
			type5HostJedisMap.put(hostInfo.toString(), jedis);
			type5HostKVWrapperMap.put(hostInfo.toString(), 
					new HashMap<Integer, KeyValueWrapper>());
		}
		type5ShardedJedis = new ShardedJedis(type5Shards, 
								Hashing.MURMUR_HASH);
		type5PipelineManager1 = new PipelineManager(type5Hosts, 
				propertyFileHandler.getPipelineQueueSize());
		type5PipelineManager4 = new PipelineManager(type5Hosts, 
				propertyFileHandler.getPipelineQueueSize());
	}

	/**
	 * Each (X,Y) in R(r) is given to this method and it inserts in
	 * type32, type4 and type5 shards.
	 *  
	 * @param key key should be of the form Yr
	 * @param value value should be of the form X
	 * @return returns the number of updates made by the insertion of 
	 * this role pair.
	 */
	public Long insertRolePair(String key, Set<String> value) throws Exception {
		String[] conceptRole = key.split(
				propertyFileHandler.getComplexAxiomSeparator());
		insertInRoleTypes(key, value, DB1, type32ShardedJedis, 
				type32HostJedisMap, type32HostKVWrapperMap);
		
		// key: Br, value: X
		// for r < s, put Ys in type4 db-1
		boolean isRolePresentOnLHS = subRoleCache.containsKey(conceptRole[1]);
		if(!isRolePresentOnLHS)
			isRolePresentOnLHS = type4ShardedJedis.exists(conceptRole[1]);
		//check again if its not in the cache but on type4 shards
		if(isRolePresentOnLHS) {
			insertInRoleTypes(key, value, DB1, type4ShardedJedis, 
					type4HostJedisMap, type4HostKVWrapperMap);
		}
		Set<String> roleChainLHS1 = roleChainLHS1Cache.get(conceptRole[1]);
		//db-0 for lhs1 chains; r < st type axioms
		if(roleChainLHS1 == null) {
			roleChainLHS1 = type5ShardedJedis.smembers(conceptRole[1]);
		}
		if((roleChainLHS1 != null) && (!roleChainLHS1.isEmpty())) {
			roleChainLHS1Cache.put(conceptRole[1], roleChainLHS1);
			JedisShardInfo shardInfo = type5ShardedJedis.getShardInfo(key);
			HostInfo hinfo = new HostInfo(
					shardInfo.getHost(), shardInfo.getPort());
			for(String val : value)
				type5PipelineManager1.psadd(
						hinfo, key, val, AxiomDB.ROLE_DB, false);
			type5PipelineManager1.psadd(hinfo, localKeys, key, 
					AxiomDB.ROLE_DB, false);
//			insertInRoleTypes(key, value, DB1, 
//					type5ShardedJedis, type5HostJedisMap, type5HostKVWrapperMap);
/*			
			for(String chain : roleChainLHS1) {
				String[] lhs2SuperRole = chain.split(
						propertyFileHandler.getComplexAxiomSeparator());
				StringBuilder conceptRoleLHS2 = 
					new StringBuilder(conceptRole[0]).
					append(propertyFileHandler.getComplexAxiomSeparator()).
					append(lhs2SuperRole[0]);
				insertInRoleTypes(conceptRoleLHS2.toString(), value, 
						DB2, type5ShardedJedis, type5HostJedisMap, 
						type5HostKVWrapperMap);
			}
*/			
		}	
		// key: Xr, value: B --> useful for processing role chains 		
		//check if this 'r' is in lhs2 of p o q < t (i.e. equals q)
		Set<String> roleChainLHS2 = roleChainLHS2Cache.get(conceptRole[1]);
		if(roleChainLHS2 == null) {
			Jedis shard = type5ShardedJedis.getShard(conceptRole[1]);
			shard.select(DB1);	//db-1 for lhs2 chains; s < rt type axioms
			roleChainLHS2 = shard.smembers(conceptRole[1]);
			shard.select(0);	//reset it back
		}
		if((roleChainLHS2 != null) && (!roleChainLHS2.isEmpty())) {
			roleChainLHS2Cache.put(conceptRole[1], roleChainLHS2);
			//put (Xq,Y) on type5 db-3;
			for(String xval : value) {
/*				
				StringBuilder conceptRoleLHS2 = new StringBuilder(xval).
					append(propertyFileHandler.getComplexAxiomSeparator()).
					append(conceptRole[1]);
				insertInRoleTypes(conceptRoleLHS2.toString(), 
						Collections.singleton(conceptRole[0]), 
						DB3, type5ShardedJedis, type5HostJedisMap, 
						type5HostKVWrapperMap);
*/						
				//put (Xp,Y) on type5 db-4
				for(String chain : roleChainLHS2) {
					String[] lhs1SuperRole = chain.split(
							propertyFileHandler.getComplexAxiomSeparator());
					StringBuilder conceptRoleLHS1 = 
						new StringBuilder(xval).
						append(propertyFileHandler.getComplexAxiomSeparator()).
						append(lhs1SuperRole[0]);
					JedisShardInfo shardInfo = 
						type5ShardedJedis.getShardInfo(
								conceptRoleLHS1.toString());
					type5PipelineManager4.psadd(
							new HostInfo(shardInfo.getHost(), 
									shardInfo.getPort()), 
							conceptRoleLHS1.toString(), conceptRole[0], 
							AxiomDB.DB4, false);
/*					
					insertInRoleTypes(conceptRoleLHS1.toString(), 
							Collections.singleton(conceptRole[0]), 
							DB4, type5ShardedJedis, type5HostJedisMap, 
							type5HostKVWrapperMap);
*/							
				}
			}
		}
		return numUpdates;
	}
	
	private Long insertInRoleTypes(String key, Set<String> value, 
			int numDB, ShardedJedis shardedJedis, Map<String, 
			Jedis> hostJedisMap, 
			Map<String, Map<Integer,KeyValueWrapper>> hostWrapperMap) 
			throws Exception {
		JedisShardInfo shardInfo = shardedJedis.getShardInfo(key);
		String hostKey = shardInfo.getHost() + ":" + shardInfo.getPort();
		Map<Integer,KeyValueWrapper> kvWrapperMap = 
			hostWrapperMap.get(hostKey);
		KeyValueWrapper kvWrapper = 
				kvWrapperMap.get(numDB);
		if(kvWrapper == null) {
			kvWrapper = new KeyValueWrapper();
			for(String s : value)
				kvWrapper.addToKeyValueList(key, s);
			kvWrapperMap.put(numDB, kvWrapper);
			if(kvWrapper.getSize() >= Constants.SCRIPT_KEY_LIMIT)
				numUpdates += kvInsertion(hostJedisMap, hostWrapperMap);
		}
		else {	
			for(String s : value)
				kvWrapper.addToKeyValueList(key, s);
			if(kvWrapper.getSize() >= Constants.SCRIPT_KEY_LIMIT)
				numUpdates += kvInsertion(hostJedisMap, hostWrapperMap);
		}
		return numUpdates;
	}
	
	protected Long kvInsertion(Map<String, Jedis> hjMap,Map<String, 
			Map<Integer,KeyValueWrapper>> hkvMap) throws Exception {
		Long numUpdates = new Long(0);
		Set<String> hostKeys = hjMap.keySet();
		for(String host : hostKeys) {
			Map<Integer,KeyValueWrapper> kvWrapperMap = hkvMap.get(host);
			Set<Entry<Integer,KeyValueWrapper>> entryWrapper = 
					kvWrapperMap.entrySet();
			for(Entry<Integer,KeyValueWrapper> entry : entryWrapper) {
				KeyValueWrapper kvWrapper = entry.getValue();
				if(!entry.getValue().keyList.isEmpty()) {
					if(kvWrapper.keyList.size() < Constants.SCRIPT_KEY_LIMIT) {
						int dbID = entry.getKey().intValue();
						hjMap.get(host).select(dbID);
						if(dbID == DB1 || dbID == DB3)
							numUpdates += (Long) hjMap.get(host).eval(
									checkAndInsertScript, 
									kvWrapper.keyList, 
									kvWrapper.valueList);
						else if(dbID == DB2 || dbID == DB4)
							numUpdates += (Long) hjMap.get(host).eval(
									insertionScript2, 
									kvWrapper.keyList, 
									kvWrapper.valueList);
						else
							throw new Exception("Unexpected dbID: " + dbID);
					}
					else {
						int j = 1;
						int startIndex = 0;
						while(j <= kvWrapper.keyList.size()) {
							if((j%Constants.SCRIPT_KEY_LIMIT == 0) || 
									(j == kvWrapper.keyList.size())) {
								int dbID = entry.getKey().intValue();
								hjMap.get(host).select(dbID);
								if(dbID == DB1 || dbID == DB3)
									numUpdates += (Long) hjMap.get(host).eval(
										checkAndInsertScript, 
										kvWrapper.keyList.subList(startIndex, j), 
										kvWrapper.valueList.subList(startIndex, j));
								else if(dbID == DB2 || dbID == DB4)
									numUpdates += (Long) hjMap.get(host).eval(
											insertionScript2, 
											kvWrapper.keyList.subList(startIndex, j), 
											kvWrapper.valueList.subList(startIndex, j));
								else
									throw new Exception("Unexpected dbID: " + dbID);
								startIndex = j;
							}
							j++;
						}
					}
					kvWrapper.keyList.clear();
					kvWrapper.valueList.clear();
				}
			}
		}
		return numUpdates;
	}
	
	protected void cleanUp() {
		localStore.disconnect();
		type32ShardedJedis.disconnect();
		for(Jedis jedis : type32HostJedisMap.values())
			jedis.disconnect();
		type4ShardedJedis.disconnect();
		for(Jedis jedis : type4HostJedisMap.values())
			jedis.disconnect();
		type5ShardedJedis.disconnect();
		for(Jedis jedis : type5HostJedisMap.values())
			jedis.disconnect();
		type5PipelineManager1.closeAll();
		type5PipelineManager4.closeAll();
	}
}
