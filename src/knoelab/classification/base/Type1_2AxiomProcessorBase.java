package knoelab.classification.base;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import knoelab.classification.init.AxiomDistributionType;
import knoelab.classification.misc.AxiomDB;
import knoelab.classification.misc.Constants;
import knoelab.classification.misc.HostInfo;
import knoelab.classification.misc.PropertyFileHandler;
import knoelab.classification.misc.ScriptsCollection;
import knoelab.classification.misc.Util;
import knoelab.classification.pipeline.PipelineManager;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Response;

public class Type1_2AxiomProcessorBase implements AxiomProcessor {

	private String scriptNConjuncts;
	private List<String> keysToAdd;
	protected Jedis resultStore;
	protected PropertyFileHandler propertyFileHandler;
	private Jedis chunkChannelHost;
	private StringBuilder hostPortTypeMessage;
	
	protected boolean isInstrumentationEnabled;
	protected long initStartTime;
	protected int maxConjuncts = 0; 
	
	public Type1_2AxiomProcessorBase(String machineName, int port) {
		propertyFileHandler = PropertyFileHandler.getInstance();
		
		isInstrumentationEnabled = 
				propertyFileHandler.isInstrumentationEnabled();
		if(isInstrumentationEnabled)
			initStartTime = System.nanoTime();
		
		HostInfo resultHostInfo = propertyFileHandler.getResultNode();
		resultStore = new Jedis(resultHostInfo.getHost(), resultHostInfo.getPort(), 
							Constants.INFINITE_TIMEOUT);
		// need to connect explicitly for lua scripting - gives a NPE if not done.
		resultStore.connect();
		keysToAdd = new ArrayList<String>();
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
		HostInfo channelHostInfo = propertyFileHandler.getChannelHost();
		chunkChannelHost = new Jedis(channelHostInfo.getHost(), 
				channelHostInfo.getPort(), Constants.INFINITE_TIMEOUT);
		hostPortTypeMessage = new StringBuilder(machineName).
			append(Constants.SEPARATOR_COLON).
			append(port).append(Constants.SEPARATOR_COLON).append(
					AxiomDistributionType.CR_TYPE1_2.toString()).
					append(Constants.SEPARATOR_RATE);
	}
	
	@Override
	public boolean processOneWorkChunk(List<String> chunkKeys, 
			PipelineManager pipelinedReader, HostInfo keysHostInfo, 
			Double currentIncrement, Jedis keysStore) throws Exception {
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
		
		for(String axiomKey : chunkKeys) 
			pipelinedReader.pZRangeByScore(keysHostInfo, axiomKey, 
					minScore, maxScore, AxiomDB.NON_ROLE_DB);
		pipelinedReader.synchAll(AxiomDB.NON_ROLE_DB);
		
		if(isInstrumentationEnabled)
			System.out.println("T12: Time taken to read axiomKey values from " + 
					keysHostInfo.toString() + ": " + Util.getElapsedTimeSecs(
							keysReadStartTime));
		
		List<Response<Set<String>>> responseList = new ArrayList<Response<Set<String>>>(
				pipelinedReader.getZrangeByScoreResponseList());
		pipelinedReader.resetSynchResponse();
		int i = 0;
		//TODO: use threading as in 2-way joins. Don't wait until everything is
		//		read(pipelinedReader)

		long applyRuleStartTime = 0;
		if(isInstrumentationEnabled)
			applyRuleStartTime = System.nanoTime();
		
		for(String axiomKey : chunkKeys) {
			if(responseList.get(i).get().isEmpty())
				axiomUpdate = false;
			else
				axiomUpdate = applyRule(axiomKey, responseList.get(i).get());
			continueProcessing = continueProcessing || axiomUpdate;
			i++;
		}
		
		if(isInstrumentationEnabled)
			System.out.println("T12: Time taken to applyRule() on the chunk: " + 
					Util.getElapsedTimeSecs(applyRuleStartTime));
		
		if(!keysToAdd.isEmpty()) {
			
			long addUpdatedKeysLocallyTime = 0;
			if(isInstrumentationEnabled)
				addUpdatedKeysLocallyTime = System.nanoTime();
			
			if(keysToAdd.size() < Constants.SCRIPT_KEY_LIMIT)
				keysStore.eval(ScriptsCollection.scriptCurrentKeysAdd, 
						Collections.singletonList(
								Constants.CURRENT_KEYS), keysToAdd);
			else {
				int j = 1;
				int startIndex = 0;
				while(j <= keysToAdd.size()) {
					if((j%Constants.SCRIPT_KEY_LIMIT == 0) || 
							(j == keysToAdd.size())) {
						keysStore.eval(ScriptsCollection.scriptCurrentKeysAdd, 
								Collections.singletonList(
										Constants.CURRENT_KEYS), 
								keysToAdd.subList(startIndex, j));
						startIndex = j;
					}
					j++;
				}
			}
			keysToAdd.clear();
			
			if(isInstrumentationEnabled)
				System.out.println("T12: Time taken to add updated keys locally: " + 
						Util.getElapsedTimeSecs(addUpdatedKeysLocallyTime));
			
		}
		return continueProcessing;
	}
	
	private boolean applyRule(String axiomKey, Set<String> axiomValue) 
	throws Exception {
		List<String> conjuncts = Util.unpackIDs(axiomKey);
		// there are n conjuncts on the LHS of the axiom 
//		long startTime = System.nanoTime();
		Long numUpdates = (Long) resultStore.eval(scriptNConjuncts, conjuncts, 
								new ArrayList<String>(axiomValue));
//		long endTime = System.nanoTime();
//		totalDiffTime = totalDiffTime + (endTime - startTime);
		if(conjuncts.size() > maxConjuncts)
			maxConjuncts = conjuncts.size();
		if(numUpdates > 0) {
			for(String superClass : axiomValue)
				keysToAdd.add(superClass);
		}		
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
		if(resultStore != null)
			resultStore.disconnect();
		if(chunkChannelHost != null)
			chunkChannelHost.disconnect();
	}
}
