package knoelab.classification.base;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Response;
import redis.clients.jedis.Tuple;
import knoelab.classification.RolePairHandler;
import knoelab.classification.init.AxiomDistributionType;
import knoelab.classification.misc.AxiomDB;
import knoelab.classification.misc.Constants;
import knoelab.classification.misc.HostInfo;
import knoelab.classification.misc.Util;
import knoelab.classification.pipeline.PipelineManager;

public class Type2AxiomProcessorBase extends RolePairHandler implements AxiomProcessor {

	protected Jedis scoreDB;
	protected HostInfo resultNodeHostInfo;
	protected PipelineManager resultNodePReader;
	private Jedis chunkChannelHost;
	private StringBuilder hostPortTypeMessage;
	
	public Type2AxiomProcessorBase(String machineName, int port) {
		super(port);
		scoreDB = new Jedis(machineName, port, Constants.INFINITE_TIMEOUT);
		scoreDB.select(AxiomDB.SCORE_DB.getDBIndex());
		resultNodeHostInfo = propertyFileHandler.getResultNode();
		resultNodePReader = new PipelineManager(Collections.singletonList(
				resultNodeHostInfo), 
						propertyFileHandler.getPipelineQueueSize());
		HostInfo channelHostInfo = propertyFileHandler.getChannelHost();
		chunkChannelHost = new Jedis(channelHostInfo.getHost(), 
				channelHostInfo.getPort(), Constants.INFINITE_TIMEOUT);
		hostPortTypeMessage = new StringBuilder(machineName).
			append(Constants.SEPARATOR_COLON).
			append(port).append(Constants.SEPARATOR_COLON).append(
					AxiomDistributionType.CR_TYPE2.toString()).
					append(Constants.SEPARATOR_RATE);
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
			numUpdates += insertRolePair(ce, xvals, true);
			//TODO: move the eval stmt to insert keyList & valueList here
			//		Do this for every key rather than all keys -- unnecessary 
			//		passing of same keys over network 
		}
		numUpdates += kvInsertion(type32HostJedisMap, type32HostKVWrapperMap);
		numUpdates += kvInsertion(type4HostJedisMap, type4HostKVWrapperMap);
		if(type5PipelineManager1 != null)
			type5PipelineManager1.synchAll(AxiomDB.ROLE_DB);
		if(type5PipelineManager4 != null)
			type5PipelineManager4.synchAll(AxiomDB.DB4);
//		numUpdates += kvInsertion(type5HostJedisMap, type5HostKVWrapperMap);
		
		return (numUpdates > 0)?true:false;
	}
	
	@Override
	public boolean processOneWorkChunk(List<String> chunkKeys, 
			PipelineManager pipelinedReader, HostInfo keysHostInfo, 
			Double currentIncrement, Jedis keyStore) throws Exception {
//		System.out.println("Axioms to process: " + chunkKeys.size());
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
			// Constants.SCORE_INCREMENT has been added to score in order to replicate 
			// the functionality of (score i.e. exclude 'score' in the zrange
			resultNodePReader.pZRangeByScoreWithScores(resultNodeHostInfo, axiomKey, 
					score+Constants.SCORE_INCREMENT, Double.POSITIVE_INFINITY, 
					AxiomDB.NON_ROLE_DB);
		}
		pipelinedReader.synchAll(AxiomDB.NON_ROLE_DB);		
		List<Response<Set<String>>> responseList = 
			pipelinedReader.getZrangeByScoreResponseList();	
		resultNodePReader.synchAll(AxiomDB.NON_ROLE_DB);
		List<Response<Set<Tuple>>> elementScoreList = 
			resultNodePReader.getZrangeByScoreWithScoresResponseList();
		
		if(isInstrumentationEnabled)
			System.out.println("T2: Time taken to read axiomKey values from " + 
					keysHostInfo.toString() + " and resultStore" + ": " + 
					Util.getElapsedTimeSecs(keysReadStartTime));
		
		long applyRuleStartTime = 0;
		if(isInstrumentationEnabled)
			applyRuleStartTime = System.nanoTime();
		
		int i = 0;
		for(String axiomKey : chunkKeys) {
			axiomUpdate = applyRule(axiomKey, responseList.get(i).get(), 
					elementScoreList.get(i).get());
			continueProcessing = continueProcessing || axiomUpdate;
			i++;
		}
		
		if(isInstrumentationEnabled)
			System.out.println("T2: Time taken to applyRule() on the chunk: " + 
					Util.getElapsedTimeSecs(applyRuleStartTime));
		
		if(i != elementScoreList.size())
			throw new Exception("i should be same as list size -- i: " + 
					i + "  lst size: " + elementScoreList.size() + 
					"  responseLst size: " + responseList.size());
		resultNodePReader.resetSynchResponse();
		pipelinedReader.resetSynchResponse();
//		keySet.clear();
		
		long postProcessingStartTime = 0;
		if(isInstrumentationEnabled)
			postProcessingStartTime = System.nanoTime();
		
		boolean domainRangeUpdates = insertDomainRangeKV();
		boolean c5ruleUpdates = insertTypeBottomKV();
		continueProcessing = continueProcessing || domainRangeUpdates 
								|| c5ruleUpdates;
		
		if(isInstrumentationEnabled)
			System.out.println("T2: Time taken for post-processing " +
					"(insertDomainRangeKV, insertTypeBottomKV): " + 
					Util.getElapsedTimeSecs(postProcessingStartTime));
		
		return continueProcessing;
	}
	
	@Override
	public void cleanUp() {
		super.cleanUp();
		if(scoreDB != null)
			scoreDB.disconnect();
		if(chunkChannelHost != null)
			chunkChannelHost.disconnect();
		resultNodePReader.closeAll();
	}
	
	@Override
	public void sendProgressMessage(double progress, int iterationCount) {
		StringBuilder progressMessage = new StringBuilder().
			append(iterationCount).append(Constants.SEPARATOR_RATE).
			append(hostPortTypeMessage).append(progress);
		chunkChannelHost.publish(Constants.PROGRESS_CHANNEL, 
				progressMessage.toString());
	}
}
