package knoelab.classification;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import knoelab.classification.init.AxiomDistributionType;
import knoelab.classification.init.EntityType;
import knoelab.classification.misc.AxiomDB;
import knoelab.classification.misc.Constants;
import knoelab.classification.misc.HostInfo;
import knoelab.classification.misc.LRUCache;
import knoelab.classification.misc.PropertyFileHandler;
import knoelab.classification.misc.ScriptsCollection;
import knoelab.classification.misc.Util;
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
	private Jedis localStore;
	private HostInfo localHostInfo;
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
	private ShardedJedis type2ShardedJedis;
	
	protected ShardedJedis type11ShardedJedis;
//	protected Map<String, KeyValueWrapper> type11HostKVWrapperMap;
	private Jedis resultStore;
	private KeyValueWrapper classAssertionKVWrapper;
	
	private final int DB0 = 0;
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
	// holds 'r' as key and its domain, range as value
	protected LRUCache<String, Map<String, String>> roleDomainRangeCache;
	private boolean foundBottom;
	private ShardedJedis typeBottomShardedJedis;
	protected Map<String, Map<String, Set<String>>> typeBottomHostKVMap;
	private String topID;
	
	protected boolean isInstrumentationEnabled;
	protected long initStartTime;
	
	public RolePairHandler(int localHostPort) {
		try {
			propertyFileHandler = PropertyFileHandler.getInstance();
			
			isInstrumentationEnabled = 
					propertyFileHandler.isInstrumentationEnabled();
			if(isInstrumentationEnabled)
				initStartTime = System.nanoTime();
			
			localKeys = propertyFileHandler.getLocalKeys();
			localHostInfo = propertyFileHandler.getLocalHostInfo();
			
			//not using localHostPort because when multiple DBs are run on one
			//machine, there was a problem. machine1 (localhost) might not be
			//running on port 6381 but machine2 (work steal process involving roles)
			//might be running on 6381.
//			localHostInfo.setPort(localHostPort);
			
			//localStore is used only to read host info which is available on
			//on any port (of any machine).
			localStore = new Jedis(localHostInfo.getHost(), 
					localHostInfo.getPort(), Constants.INFINITE_TIMEOUT);		
			subRoleCache = new LRUCache<String, Set<String>>(1000);
			roleChainLHS1Cache = new LRUCache<String, Set<String>>(100);
			roleChainLHS2Cache = new LRUCache<String, Set<String>>(100);
			numUpdates = new Long(0);
			roleDomainRangeCache = new LRUCache<String, Map<String, String>>(
											Constants.DOMAIN_RANGE_CACHE_CAPACITY);
			topID = Util.getPackedID(Constants.TOP_ID, EntityType.CLASS);
			String foundBottomStr = localStore.get(Constants.FOUND_BOTTOM);
			if(foundBottomStr.equals("0"))
				foundBottom = false;
			else if(foundBottomStr.equals("1"))
				foundBottom = true;
			
			HostInfo resultNode = propertyFileHandler.getResultNode();
			resultStore = new Jedis(resultNode.getHost(), resultNode.getPort(), 
					Constants.INFINITE_TIMEOUT);
			resultStore.connect();
			classAssertionKVWrapper = new KeyValueWrapper();
			
	//		initType11Shards();
			initTypeBottomShards();
			initType2Shards();
			initType32Shards();
			initType4Shards();
			initType5Shards();
		}catch (Exception e) {
			e.printStackTrace();
		}
		
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
/*	
	private void initType11Shards() {
		Set<String> type11Hosts = localStore.smembers(
				AxiomDistributionType.CR_TYPE1_1.toString());
		List<JedisShardInfo> type11Shards = new ArrayList<JedisShardInfo>();
		List<HostInfo> type11HostInfoList = new ArrayList<HostInfo>();
		for(String host : type11Hosts) {
			String[] hostPort = host.split(":");
			int port = Integer.parseInt(hostPort[1]);
			type11HostInfoList.add(new HostInfo(hostPort[0], port));
			type11Shards.add(new JedisShardInfo(hostPort[0], port, 
					Constants.INFINITE_TIMEOUT));
		}
		type11ShardedJedis = new ShardedJedis(type11Shards, 
											Hashing.MURMUR_HASH);
		type11HostKVWrapperMap = new HashMap<String, KeyValueWrapper>();
	}
*/	
	private void initType2Shards() {
		Set<String> type2Hosts = localStore.smembers(
				AxiomDistributionType.CR_TYPE2.toString());
		if(type2Hosts.isEmpty()) {
			type2ShardedJedis = null;
			return;
		}
		List<JedisShardInfo> type2Shards = new ArrayList<JedisShardInfo>();
		for(String host : type2Hosts) {
			String[] hostPort = host.split(":");
			int port = Integer.parseInt(hostPort[1]);
			type2Shards.add(new JedisShardInfo(hostPort[0], port, 
					Constants.INFINITE_TIMEOUT));
		}
		type2ShardedJedis = new ShardedJedis(type2Shards, Hashing.MURMUR_HASH);
	}
	
	private void initTypeBottomShards() {
		if(!foundBottom) {
			typeBottomShardedJedis = null;
			return;
		}
		Set<String> typeBottomHosts = localStore.smembers(
				AxiomDistributionType.CR_TYPE_BOTTOM.toString());
		List<JedisShardInfo> typeBottomShards = new ArrayList<JedisShardInfo>();
		List<HostInfo> typeBottomHostInfoList = new ArrayList<HostInfo>();
		typeBottomHostKVMap = new HashMap<String, Map<String, Set<String>>>();
		for(String host : typeBottomHosts) {
			String[] hostPort = host.split(":");
			int shardPort = Integer.parseInt(hostPort[1]);
			HostInfo hinfo = new HostInfo(hostPort[0], shardPort);
			typeBottomHostInfoList.add(hinfo);
			typeBottomShards.add(new JedisShardInfo(hostPort[0], shardPort, 
					Constants.INFINITE_TIMEOUT));
			typeBottomHostKVMap.put(hinfo.toString(), 
					new HashMap<String, Set<String>>());
		}
		typeBottomShardedJedis = new ShardedJedis(typeBottomShards, 
											Hashing.MURMUR_HASH);
	}
	
	private void initType32Shards() {
		Set<String> type32Hosts = localStore.smembers(
						AxiomDistributionType.CR_TYPE3_2.toString());
		if(type32Hosts.isEmpty()) {
			type32ShardedJedis = null;
			type32HostJedisMap = null;
			type32HostKVWrapperMap = null;
			return;
		}
		List<JedisShardInfo> type32Shards = new ArrayList<JedisShardInfo>();
		type32HostKVWrapperMap = 
			new HashMap<String, Map<Integer,KeyValueWrapper>>();
		type32HostJedisMap = new HashMap<String, Jedis>();
		for(String hostInfo : type32Hosts) {
			String[] hostPort = hostInfo.split(":");
			int port = Integer.parseInt(hostPort[1]);
			type32Shards.add(new JedisShardInfo(hostPort[0], 
					port, Constants.INFINITE_TIMEOUT));
			Jedis jedis = new Jedis(hostPort[0], 
					port, Constants.INFINITE_TIMEOUT);
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
		Set<String> type4Hosts = localStore.smembers(
						AxiomDistributionType.CR_TYPE4.toString());
		List<JedisShardInfo> type4Shards = new ArrayList<JedisShardInfo>();
		if(type4Hosts.isEmpty()) {
			type4ShardedJedis = null;
			type4HostJedisMap = null;
			type4HostKVWrapperMap = null;
			return;
		}
		type4HostKVWrapperMap = 
			new HashMap<String, Map<Integer,KeyValueWrapper>>();
		type4HostJedisMap = new HashMap<String, Jedis>();
		for(String hostInfo : type4Hosts) {
			String[] hostPort = hostInfo.split(":");
			int port = Integer.parseInt(hostPort[1]);
			type4Shards.add(new JedisShardInfo(hostPort[0], 
					port, Constants.INFINITE_TIMEOUT));
			Jedis jedis = new Jedis(hostPort[0], 
					port, Constants.INFINITE_TIMEOUT);
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
		Set<String> type5Hosts = localStore.smembers(
					AxiomDistributionType.CR_TYPE5.toString());
		if(type5Hosts.isEmpty()) {
			type5ShardedJedis = null;
			type5PipelineManager1 = type5PipelineManager4 = null;
			return;
		}
		List<HostInfo> type5HostsInfo = new ArrayList<HostInfo>();
		for(String hostInfo : type5Hosts) {
			String[] hostPort = hostInfo.split(":");
			int port = Integer.parseInt(hostPort[1]);
			HostInfo hinfo = new HostInfo(hostPort[0], port);
			System.out.println("Type5 shard: " + hinfo.toString());
			type5HostsInfo.add(hinfo);
			type5Shards.add(new JedisShardInfo(hostPort[0], 
					port, Constants.INFINITE_TIMEOUT));
			Jedis jedis = new Jedis(hostPort[0], 
					port, Constants.INFINITE_TIMEOUT);
			// need to connect explicitly for lua scripting - gives a NPE if not done.
			jedis.connect();
			type5HostJedisMap.put(hostInfo.toString(), jedis);
			type5HostKVWrapperMap.put(hostInfo.toString(), 
					new HashMap<Integer, KeyValueWrapper>());
		}
		type5ShardedJedis = new ShardedJedis(type5Shards, 
								Hashing.MURMUR_HASH);
		type5PipelineManager1 = new PipelineManager(type5HostsInfo, 
				propertyFileHandler.getPipelineQueueSize());
		type5PipelineManager4 = new PipelineManager(type5HostsInfo, 
				propertyFileHandler.getPipelineQueueSize());
	}

	/**
	 * Each (X,Y) in R(r) is given to this method and it inserts in
	 * type32, type4 and type5 shards.
	 *  
	 * @param key key should be of the form Yr
	 * @param value value should be of the form X
	 * @param bottomInsert related to CR5, rule on \bot
	 * @return returns the number of updates made by the insertion of 
	 * this role pair.
	 */
	public Long insertRolePair(String key, Set<String> value, 
			boolean bottomInsert) throws Exception {
		List<String> conceptRole = Util.unpackIDs(key);
		//given r(a,b) -- check domain/range of r and create class assertions
		insertClassAssertions(conceptRole.get(0), conceptRole.get(1), value);
		if(bottomInsert && foundBottom) {
			//insert (zadd) key:Y, value:X into TypeBottom nodes
			JedisShardInfo shardInfo = typeBottomShardedJedis.getShardInfo(
											conceptRole.get(0));
			HostInfo hinfo = new HostInfo(shardInfo.getHost(), shardInfo.getPort());
			Map<String, Set<String>> map = typeBottomHostKVMap.get(hinfo.toString());
			Set<String> values = map.get(conceptRole.get(0));
			if(values == null) {
				values = new HashSet<String>();
				values.addAll(value);
				map.put(conceptRole.get(0), values);
			}
			else
				values.addAll(value);
		}
		
		if(type32ShardedJedis != null)
			insertInRoleTypes(key, value, DB1, type32ShardedJedis, 
				type32HostJedisMap, type32HostKVWrapperMap);
		
		// key: Br, value: X
		// for r < s, put Ys in type4 db-1
		if(type4ShardedJedis != null) {
			Set<String> superRoles = subRoleCache.get(conceptRole.get(1));
			//check again if its not in the cache but on type4 shards
			if(superRoles == null || superRoles.isEmpty())
				superRoles = type4ShardedJedis.smembers(conceptRole.get(1));
			if(superRoles != null && (!superRoles.isEmpty())) {
				subRoleCache.put(conceptRole.get(1), superRoles);
				for(String superRole : superRoles)
					insertClassAssertions(conceptRole.get(0), superRole, value);
				insertInRoleTypes(key, value, DB0, type4ShardedJedis, 
						type4HostJedisMap, type4HostKVWrapperMap);
			}
		}
		if(type5ShardedJedis == null)
			return numUpdates;
		
		Set<String> roleChainLHS1 = roleChainLHS1Cache.get(conceptRole.get(1));
		//db-0 for lhs1 chains; r < st type axioms
		if(roleChainLHS1 == null) 
			roleChainLHS1 = type5ShardedJedis.smembers(conceptRole.get(1));
		
		if((roleChainLHS1 != null) && (!roleChainLHS1.isEmpty())) {
			roleChainLHS1Cache.put(conceptRole.get(1), roleChainLHS1);
			JedisShardInfo shardInfo = type5ShardedJedis.getShardInfo(key);
			HostInfo hinfo = new HostInfo(
					shardInfo.getHost(), shardInfo.getPort());
			for(String val : value)
				type5PipelineManager1.psadd(
						hinfo, key, val, AxiomDB.ROLE_DB, false);
			type5PipelineManager1.psadd(hinfo, localKeys, key, 
					AxiomDB.ROLE_DB, false);		
		}
		else {
			if(conceptRole.get(1).equals("02192")) {
				System.out.println("02192 not found in Type5. Key: " + key + 
						"  value: " + value);
			}
		}
		// key: Xr, value: B --> useful for processing role chains 		
		//check if this 'r' is in lhs2 of p o q < t (i.e. equals q)
		Set<String> roleChainLHS2 = roleChainLHS2Cache.get(conceptRole.get(1));
		if(roleChainLHS2 == null) {
			Jedis shard = type5ShardedJedis.getShard(conceptRole.get(1));
			shard.select(DB1);	//db-1 for lhs2 chains; s < rt type axioms
			roleChainLHS2 = shard.smembers(conceptRole.get(1));
			shard.select(0);	//reset it back
		}
		/*Way this works -- r \circ s < t and r \circ p < q
		 * (X,Y) \in r, (Y,Z) \in s and p.
		 * shard = hash(Yr); so both go to same shard. either t or p doesn't matter
		 * (X,Z) \in t and (X,Z) \in q 
		 */
		if((roleChainLHS2 != null) && (!roleChainLHS2.isEmpty())) {
			roleChainLHS2Cache.put(conceptRole.get(1), roleChainLHS2);
			for(String xval : value) {						
				//put (Xp,Y) on type5 db-4
				for(String chain : roleChainLHS2) {
					List<String> lhs1SuperRole = Util.unpackIDs(chain);
					StringBuilder conceptRoleLHS1 = 
						new StringBuilder(xval).append(lhs1SuperRole.get(0));
					JedisShardInfo shardInfo = 
						type5ShardedJedis.getShardInfo(
								conceptRoleLHS1.toString());
					type5PipelineManager4.psadd(
							new HostInfo(shardInfo.getHost(), 
									shardInfo.getPort()), 
							conceptRoleLHS1.toString(), conceptRole.get(0), 
							AxiomDB.DB4, false);			
				}
			}
		}
		return numUpdates;
	}
	
	/**
	 * for every r(a, b); check whether r has
	 * any domain/range property assertions defined. If so, add class assertion
	 * axioms.
	 *  
	 * @param conceptRole Yr with the possibility of Y being a datatype
	 * @param values set of X with the possibility of X being a datatype
	 */
	private void insertClassAssertions(String concept, String role, 
			Set<String> values) throws Exception {
		//check if this 'r' has any domain/range defined
		Map<String, String> domainRangeMap = roleDomainRangeCache.get(role);
		if(domainRangeMap == null) {			
			Jedis jedis = type2ShardedJedis.getShard(role);
			jedis.select(AxiomDB.DB5.getDBIndex());
			domainRangeMap = jedis.hgetAll(role);
//			localStore.select(0);
			if(domainRangeMap == null)
				return;
			roleDomainRangeCache.put(role, domainRangeMap);				
		}		
//		String yval = conceptRole.get(0);
		//last digit contains the type 
		int entityType = Character.getNumericValue(concept.charAt(
				concept.length()-1));
		if(EntityType.getEntityType(entityType) != EntityType.DATATYPE) {
			String rangeID = domainRangeMap.get(Constants.RANGE_FIELD);
			//concept should not be top or else it would lead to top \sqsubseteq Concept
			//which is invalid
			if(rangeID != null && !concept.equals(topID)) 
				classAssertionKVWrapper.addToKeyValueList(concept, rangeID);
		}
		String domainID = domainRangeMap.get(Constants.DOMAIN_FIELD);
		if(domainID == null)
			return;
		for(String domainIndividual : values) {
			if(domainIndividual.equals(topID))
				continue;
			entityType = Character.getNumericValue(domainIndividual.charAt(
					domainIndividual.length()-1));
			if(EntityType.getEntityType(entityType) != EntityType.DATATYPE) 
				classAssertionKVWrapper.addToKeyValueList(domainIndividual, domainID);
		}
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
//		for(String s : value)
//			System.out.println("RolePair; Key: " + key + "  value: " +s);
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
				if(!entry.getValue().getKeyList().isEmpty()) {
					if(kvWrapper.getKeyList().size() < Constants.SCRIPT_KEY_LIMIT) {
						int dbID = entry.getKey().intValue();
						hjMap.get(host).select(dbID);
						if(dbID == DB0 || dbID == DB1 || dbID == DB3)
							numUpdates += (Long) hjMap.get(host).eval(
									checkAndInsertScript, 
									kvWrapper.getKeyList(), 
									kvWrapper.getValueList());
						else if(dbID == DB2 || dbID == DB4)
							numUpdates += (Long) hjMap.get(host).eval(
									insertionScript2, 
									kvWrapper.getKeyList(), 
									kvWrapper.getValueList());
						else
							throw new Exception("Unexpected dbID: " + dbID);
					}
					else {
						int j = 1;
						int startIndex = 0;
						while(j <= kvWrapper.getKeyList().size()) {
							if((j%Constants.SCRIPT_KEY_LIMIT == 0) || 
									(j == kvWrapper.getKeyList().size())) {
								int dbID = entry.getKey().intValue();
								hjMap.get(host).select(dbID);
								if(dbID == DB0 || dbID == DB1 || dbID == DB3)
									numUpdates += (Long) hjMap.get(host).eval(
										checkAndInsertScript, 
										kvWrapper.getKeyList().subList(startIndex, j), 
										kvWrapper.getValueList().subList(startIndex, j));
								else if(dbID == DB2 || dbID == DB4)
									numUpdates += (Long) hjMap.get(host).eval(
											insertionScript2, 
											kvWrapper.getKeyList().subList(startIndex, j), 
											kvWrapper.getValueList().subList(startIndex, j));
								else
									throw new Exception("Unexpected dbID: " + dbID);
								startIndex = j;
							}
							j++;
						}
					}
					kvWrapper.clear();
				}
			}
		}
		return numUpdates;
	}
	
	protected boolean insertDomainRangeKV() {
		Long numUpdates = new Long(0);
		if(classAssertionKVWrapper.getSize() == 0)
			return false;
		if(classAssertionKVWrapper.getKeyList().size() < Constants.SCRIPT_KEY_LIMIT) {					
			numUpdates += (Long) resultStore.eval(
					ScriptsCollection.insertClassAssertions1, 
					classAssertionKVWrapper.getKeyList(), 
					classAssertionKVWrapper.getValueList());
		}
		else {
			int j = 1;
			int startIndex = 0;
			while(j <= classAssertionKVWrapper.getKeyList().size()) {
				if((j%Constants.SCRIPT_KEY_LIMIT == 0) || 
						(j == classAssertionKVWrapper.getKeyList().size())) {
					numUpdates += (Long) resultStore.eval(
							ScriptsCollection.insertClassAssertions1, 
							classAssertionKVWrapper.getKeyList().subList(startIndex, j), 
							classAssertionKVWrapper.getValueList().subList(startIndex, j));
					startIndex = j;
				}
				j++;
			}
		}
		classAssertionKVWrapper.clear();
		return (numUpdates > 0)?true:false;
	}
	
	protected boolean insertTypeBottomKV() {
		if(!foundBottom)
			return false;
		Set<String> keySet = typeBottomHostKVMap.keySet();
		Long numUpdates = new Long(0);
		for(String host : keySet) {
			String[] hostPort = host.split(":");
			Jedis jedis = new Jedis(hostPort[0], Integer.parseInt(hostPort[1]), 
									Constants.INFINITE_TIMEOUT);
			jedis.connect();
			Map<String, Set<String>> kvMap = typeBottomHostKVMap.get(host);
			Set<String> keys = kvMap.keySet();
			for(String key : keys) {
				Set<String> values = kvMap.get(key);
				numUpdates += (Long) jedis.eval(
						ScriptsCollection.insertInBottom, 
						Collections.singletonList(key), 
						new ArrayList<String>(values));
			}
			jedis.disconnect();
			kvMap.clear();
		}
		return (numUpdates > 0)?true:false;
	}
	
	protected void cleanUp() {
		localStore.disconnect();
		resultStore.disconnect();
		if(type32ShardedJedis != null)
			type32ShardedJedis.disconnect();
		for(Jedis jedis : type32HostJedisMap.values())
			jedis.disconnect();
		if(type4ShardedJedis != null)
			type4ShardedJedis.disconnect();
		for(Jedis jedis : type4HostJedisMap.values())
			jedis.disconnect();
		if(type5ShardedJedis != null)
			type5ShardedJedis.disconnect();
		for(Jedis jedis : type5HostJedisMap.values())
			jedis.disconnect();
		if(type5PipelineManager1 != null)
			type5PipelineManager1.closeAll();
		if(type5PipelineManager4 != null)
			type5PipelineManager4.closeAll();
		if(type11ShardedJedis != null)
			type11ShardedJedis.disconnect();
		if(typeBottomShardedJedis != null)
			typeBottomShardedJedis.disconnect();
		if(type2ShardedJedis != null)
			type2ShardedJedis.disconnect();
	}
}
