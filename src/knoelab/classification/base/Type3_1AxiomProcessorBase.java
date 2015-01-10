package knoelab.classification.base;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import knoelab.classification.KeyValueWrapper;
import knoelab.classification.init.AxiomDistributionType;
import knoelab.classification.init.EntityType;
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

public class Type3_1AxiomProcessorBase implements AxiomProcessor {
	
	protected PropertyFileHandler propertyFileHandler;
	protected Jedis scoreDB;
	private Jedis chunkChannelHost;
	private StringBuilder hostPortTypeMessage;
	private String checkAndInsertScript;
	private PipelineManager resultNodePReader;
	protected HostInfo resultNode;
	private ShardedJedis type3_2ShardedJedis;
	private Map<String, Jedis> hostJedisMap;
	private Map<String, KeyValueWrapper> hostKVWrapperMap;
	
	protected boolean isInstrumentationEnabled;
	protected long initStartTime;
	
	public Type3_1AxiomProcessorBase(String machineName, int port) {
		propertyFileHandler = PropertyFileHandler.getInstance();
		
		isInstrumentationEnabled = 
				propertyFileHandler.isInstrumentationEnabled();
		if(isInstrumentationEnabled)
			initStartTime = System.nanoTime();
		
		HostInfo channelHostInfo = propertyFileHandler.getChannelHost();
		chunkChannelHost = new Jedis(channelHostInfo.getHost(), 
				channelHostInfo.getPort(), Constants.INFINITE_TIMEOUT);
		chunkChannelHost = new Jedis(channelHostInfo.getHost(), 
				channelHostInfo.getPort(), Constants.INFINITE_TIMEOUT);
		hostPortTypeMessage = new StringBuilder(machineName).
			append(Constants.SEPARATOR_COLON).
			append(port).append(Constants.SEPARATOR_COLON).append(
					AxiomDistributionType.CR_TYPE3_1.toString()).
					append(Constants.SEPARATOR_RATE);
		scoreDB = new Jedis(machineName, port, Constants.INFINITE_TIMEOUT);
		Set<String> type32Hosts = scoreDB.zrange(
				AxiomDistributionType.CR_TYPE3_2.toString(), 
				Constants.RANGE_BEGIN, Constants.RANGE_END);
		scoreDB.select(AxiomDB.SCORE_DB.getDBIndex());	
		resultNode = propertyFileHandler.getResultNode();
		resultNodePReader = new PipelineManager(Collections.singletonList(resultNode), 
				propertyFileHandler.getPipelineQueueSize());
		List<JedisShardInfo> type32Shards = new ArrayList<JedisShardInfo>();
		hostJedisMap = new HashMap<String, Jedis>();
		hostKVWrapperMap = new HashMap<String, KeyValueWrapper>();
		for(String hostInfo : type32Hosts) {
			String[] hostPort = hostInfo.split(":");
			int type32Port = Integer.parseInt(hostPort[1]);
			type32Shards.add(new JedisShardInfo(hostPort[0], 
					type32Port, Constants.INFINITE_TIMEOUT));
			Jedis jedis = new Jedis(hostPort[0], type32Port, 
					Constants.INFINITE_TIMEOUT);
			// need to connect explicitly for lua scripting - gives a NPE if not done.
			jedis.connect();
			hostJedisMap.put(hostInfo.toString(), jedis);
			hostKVWrapperMap.put(hostInfo.toString(), new KeyValueWrapper());
		}
		type3_2ShardedJedis = new ShardedJedis(type32Shards, 
									Hashing.MURMUR_HASH);
		
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
	}

	@Override
	public boolean processOneWorkChunk(List<String> chunkKeys,
			PipelineManager pipelinedReader, HostInfo keysHostInfo,
			Double currentIncrement, Jedis keyStore) throws Exception {
		boolean continueProcessing = false;
		boolean axiomUpdate;
		//read axioms belonging to current increment only (first iteration)
		//(previous ones are not required). On subsequent iterations read whole
		//set -- specially for incremental reasoning
		double minScore, maxScore;
		if(currentIncrement < 0) {
			minScore = Double.NEGATIVE_INFINITY;
			maxScore = Double.POSITIVE_INFINITY;
		}
		else {
			minScore = currentIncrement;
			maxScore = currentIncrement;
		}	
		
		long keysReadStartTime = 0;
		if(isInstrumentationEnabled)
			keysReadStartTime = System.nanoTime();
		
		for(String axiomKey : chunkKeys) {
			pipelinedReader.pZRangeByScore(keysHostInfo, axiomKey, 
					minScore, maxScore, AxiomDB.NON_ROLE_DB);
			double score = Util.getScore(scoreDB, axiomKey);
			// SCORE_INCREMENT has been added to score in order to replicate the 
			// functionality of (score i.e. exclude 'score' in the zrange
			resultNodePReader.pZRangeByScoreWithScores(resultNode, axiomKey, 
					score+Constants.SCORE_INCREMENT, Double.POSITIVE_INFINITY, 
					AxiomDB.NON_ROLE_DB);
		}
		pipelinedReader.synchAll(AxiomDB.NON_ROLE_DB);
		resultNodePReader.synchAll(AxiomDB.NON_ROLE_DB);
		
		if(isInstrumentationEnabled)
			System.out.println("T31: Time taken to read axiomKey values from " + 
					keysHostInfo.toString() + " and resultStore" + ": " + 
					Util.getElapsedTimeSecs(keysReadStartTime));
		
		List<Response<Set<String>>> responseList = 
			pipelinedReader.getZrangeByScoreResponseList();	
		List<Response<Set<Tuple>>> zrangeResponseList = 
			resultNodePReader.getZrangeByScoreWithScoresResponseList();
		
		long applyRuleStartTime = 0;
		if(isInstrumentationEnabled)
			applyRuleStartTime = System.nanoTime();
		
		int i = 0;				
		for(String axiomKey : chunkKeys) {
			axiomUpdate = applyRule(axiomKey, responseList.get(i).get(), 
					zrangeResponseList.get(i).get());
			continueProcessing = continueProcessing || axiomUpdate;
			i++;
		}
		
		if(isInstrumentationEnabled)
			System.out.println("T31: Time taken to applyRule() on the chunk: " + 
					Util.getElapsedTimeSecs(applyRuleStartTime));
		
		chunkKeys.clear();
		if(i != zrangeResponseList.size())
			throw new Exception("i should be same as list size");
		resultNodePReader.resetSynchResponse();
		pipelinedReader.resetSynchResponse();
		return continueProcessing;
	}
	
	private boolean applyRule(String axiomKey, Set<String> superClassRoles, 
			Set<Tuple> yvalues) throws Exception {		
		double nextMinScore = 0;
		Long numUpdates = new Long(0);
		Set<String> hostKeys = hostJedisMap.keySet();
		int entityType = Character.getNumericValue(axiomKey.charAt(
				axiomKey.length()-1));
		if(EntityType.getEntityType(entityType) == EntityType.DATATYPE) {
			//for datatype, there is no S(X), so provide a default one
			Tuple t = new Tuple(axiomKey, -1.0);
			if(yvalues == null)
				yvalues = new HashSet<Tuple>();
			yvalues.add(t);
		}		
		for(String classRole : superClassRoles) {
			List<String> superClassRole = Util.unpackIDs(classRole);
			for(Tuple yScore : yvalues) {
				// key: Yr, value: B
				
				StringBuilder key = new StringBuilder(yScore.getElement()).
										append(superClassRole.get(1));
				JedisShardInfo shardInfo = type3_2ShardedJedis.getShardInfo(key.toString());
				String hostKey = shardInfo.getHost() + ":" + shardInfo.getPort();				
				KeyValueWrapper kvWrapper = hostKVWrapperMap.get(hostKey);
				kvWrapper.addToKeyValueList(key.toString(), superClassRole.get(0));		
				if(yScore.getScore() > nextMinScore)
					nextMinScore = yScore.getScore();
			}
			for(String host : hostKeys) {
				KeyValueWrapper kvWrapper = hostKVWrapperMap.get(host);
				if(!kvWrapper.getKeyList().isEmpty()) {
					try {
						numUpdates += (Long) hostJedisMap.get(host).eval(
								checkAndInsertScript, kvWrapper.getKeyList(), 
								kvWrapper.getValueList());
					}catch(Exception e) {
						e.printStackTrace();
					}
				}
				kvWrapper.clear();
			}
		}
		
		Util.setScore(scoreDB, axiomKey, nextMinScore);
		return (numUpdates > 0)?true:false;		
	}

	@Override
	public void sendProgressMessage(double progress, int iterationCount) {
		StringBuilder progressMessage = new StringBuilder().
			append(iterationCount).append(Constants.SEPARATOR_RATE).
			append(hostPortTypeMessage).append(progress);
		chunkChannelHost.publish(Constants.PROGRESS_CHANNEL, 
				progressMessage.toString());
	}

	@Override
	public void cleanUp() {
		if(chunkChannelHost != null)
			chunkChannelHost.disconnect();
		if(scoreDB != null)
			scoreDB.disconnect();
		resultNodePReader.closeAll();
		if(type3_2ShardedJedis != null)
			type3_2ShardedJedis.disconnect();
		if(hostJedisMap != null) {
			Collection<Jedis> jedisList = hostJedisMap.values();
			for(Jedis jedis : jedisList)
				jedis.disconnect();
		}
	}
}