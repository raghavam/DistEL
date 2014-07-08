package knoelab.classification.base;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import knoelab.classification.RolePairHandler;
import knoelab.classification.init.AxiomDistributionType;
import knoelab.classification.misc.AxiomDB;
import knoelab.classification.misc.Constants;
import knoelab.classification.misc.HostInfo;
import knoelab.classification.misc.Util;
import knoelab.classification.pipeline.PipelineManager;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Response;
import redis.clients.jedis.Tuple;

public class Type4AxiomProcessorBase extends RolePairHandler implements AxiomProcessor {
	
	protected Jedis scoreDB;
	private Jedis chunkChannelHost;
	private StringBuilder hostPortTypeMessage;
	
	public Type4AxiomProcessorBase(String machineName, int port) {
		super(port);
		scoreDB = new Jedis(machineName, port, Constants.INFINITE_TIMEOUT);
		scoreDB.select(2);
		HostInfo channelHostInfo = propertyFileHandler.getChannelHost();
		chunkChannelHost = new Jedis(channelHostInfo.getHost(), 
				channelHostInfo.getPort(), Constants.INFINITE_TIMEOUT);
		hostPortTypeMessage = new StringBuilder(machineName).
			append(Constants.SEPARATOR_COLON).
			append(port).append(Constants.SEPARATOR_COLON).append(
					AxiomDistributionType.CR_TYPE4.toString()).
					append(Constants.SEPARATOR_RATE);
	}

	private boolean applyRule(String axiomKey, Set<Tuple> xvalTuples) 
	throws Exception {
		numUpdates = new Long(0);
		List<String> conceptRole = Util.unpackIDs(axiomKey); 
		Set<String> superRoles = subRoleCache.get(conceptRole.get(1));
		if(superRoles == null)
			superRoles = type4ShardedJedis.smembers(conceptRole.get(1));
		if(superRoles == null)
			throw new Exception("r in r < s now found: " + 
					conceptRole.get(1));
		subRoleCache.put(conceptRole.get(1), superRoles);
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
				new StringBuilder(conceptRole.get(0)).append(s);
			numUpdates += insertRolePair(conceptSuperRole.toString(), xvals, 
					false);
		}
		xvals.clear();
		numUpdates += kvInsertion(type32HostJedisMap, type32HostKVWrapperMap);
		numUpdates += kvInsertion(type4HostJedisMap, type4HostKVWrapperMap);
		if(type5PipelineManager1 != null)
			type5PipelineManager1.synchAll(AxiomDB.ROLE_DB);
		if(type5PipelineManager4 != null)
			type5PipelineManager4.synchAll(AxiomDB.DB4);
		//	numUpdates += kvInsertion(type5HostJedisMap, type5HostKVWrapperMap);
		return (numUpdates > 0)?true:false;
	}
	
	@Override
	public boolean processOneWorkChunk(List<String> chunkKeys,
			PipelineManager pipelinedReader, HostInfo keysHostInfo,
			Double currentIncrement, Jedis keyStore) throws Exception {
		double nextMinScore;
		boolean continueProcessing = false;
		boolean axiomUpdate;
		AxiomDB axiomDB = AxiomDB.NON_ROLE_DB;
		
		long keysReadStartTime = 0;
		if(isInstrumentationEnabled)
			keysReadStartTime = System.nanoTime();
		
		for(String key : chunkKeys) {
			nextMinScore = Util.getScore(scoreDB, key);
			nextMinScore += Constants.SCORE_INCREMENT;
			// SCORE_INCREMENT has been added to score in order to replicate the 
			// functionality of (score i.e. exclude 'score' in the zrange	
			pipelinedReader.pZRangeByScoreWithScores(keysHostInfo, 
					key, nextMinScore, 
					Double.POSITIVE_INFINITY, axiomDB);
		}
		pipelinedReader.synchAll(axiomDB);
		
		if(isInstrumentationEnabled)
			System.out.println("T4: Time taken to read axiomKey values from " + 
					keysHostInfo.toString() + ": " + Util.getElapsedTimeSecs(
							keysReadStartTime));
		
		List<Response<Set<Tuple>>> zrangeResponseList = 
			pipelinedReader.getZrangeByScoreWithScoresResponseList();
		
		long applyRuleStartTime = 0;
		if(isInstrumentationEnabled)
			applyRuleStartTime = System.nanoTime();
		
		int i = 0;
		for(String key : chunkKeys) {
			axiomUpdate = applyRule(key, zrangeResponseList.get(i).get());
			continueProcessing = continueProcessing || axiomUpdate;
			i++;
		}
		
		if(isInstrumentationEnabled)
			System.out.println("T4: Time taken to applyRule() on the chunk: " + 
					Util.getElapsedTimeSecs(applyRuleStartTime));
		
		chunkKeys.clear();
		pipelinedReader.resetSynchResponse();
		
		long postProcessingStartTime = 0;
		if(isInstrumentationEnabled)
			postProcessingStartTime = System.nanoTime();
		
		boolean domainRangeUpdates = insertDomainRangeKV();
		continueProcessing = continueProcessing || domainRangeUpdates;	
		
		if(isInstrumentationEnabled)
			System.out.println("T4: Time taken for post-processing " +
					"(insertDomainRangeKV): " + 
					Util.getElapsedTimeSecs(postProcessingStartTime));
		
		return continueProcessing;
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
		super.cleanUp();
		if(scoreDB != null)
			scoreDB.disconnect();
		if(chunkChannelHost != null)
			chunkChannelHost.disconnect();
	}
}
