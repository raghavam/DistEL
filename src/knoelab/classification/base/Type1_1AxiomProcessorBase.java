package knoelab.classification.base;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import knoelab.classification.init.AxiomDistributionType;
import knoelab.classification.init.EntityType;
import knoelab.classification.misc.AxiomDB;
import knoelab.classification.misc.Constants;
import knoelab.classification.misc.HostInfo;
import knoelab.classification.misc.PropertyFileHandler;
import knoelab.classification.misc.ScriptsCollection;
import knoelab.classification.misc.Util;
import knoelab.classification.pipeline.PipelineManager;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Response;

public class Type1_1AxiomProcessorBase implements AxiomProcessor {

	private String scriptSingleConcept =  
		"local elementScore = redis.call('ZRANGE', " +
			"KEYS[1], -1, -1, 'WITHSCORES') " +
		"local unique = 0 " +
		"local t= {} " +
		"local minScore = table.remove(ARGV) " +
		"local toBeAddedList = redis.call(" +
			"'ZRANGEBYSCORE', KEYS[1], minScore, '+inf') " +
		"local escore " +
		"local score " +
		"local ret " +
		"local unique = 0 " +
		"for index1,value1 in pairs(ARGV) do " +
			"escore = redis.call('ZRANGE', value1, -1, -1, 'WITHSCORES') " +
			"score = escore[2] + " + Constants.SCORE_INCREMENT + " " +
			"for index2,value2 in pairs(toBeAddedList) do " +
				"if(not redis.call('ZSCORE', value1, value2)) then " +
					"ret = redis.call('ZADD', value1, score, value2) " +
					"unique = unique + ret " +
				"end " +
			"end " +
		"end " +
		"t[1] = tostring(elementScore[2]) " +
		"t[2] = tostring(unique) " +
		"return t ";
	protected Jedis resultStore;
	protected Jedis scoreDB;
	private List<String> keysToAdd;
	private Jedis chunkChannelHost;
	protected PropertyFileHandler propertyFileHandler;
	private StringBuilder hostPortTypeMessage;
	
	protected boolean isInstrumentationEnabled;
	protected long initStartTime;
	
	public Type1_1AxiomProcessorBase(String machineName, int port) {
		propertyFileHandler = PropertyFileHandler.getInstance();
		
		isInstrumentationEnabled = 
				propertyFileHandler.isInstrumentationEnabled();
		if(isInstrumentationEnabled)
			initStartTime = System.nanoTime();
		
		scoreDB = new Jedis(machineName, port, 
				Constants.INFINITE_TIMEOUT);
		scoreDB.select(AxiomDB.SCORE_DB.getDBIndex());
		HostInfo resultHostInfo = propertyFileHandler.getResultNode();
		resultStore = new Jedis(resultHostInfo.getHost(), resultHostInfo.getPort(), 
							Constants.INFINITE_TIMEOUT);
		// need to connect explicitly for lua scripting - gives a NPE if not done.
		resultStore.connect();
		HostInfo channelHostInfo = propertyFileHandler.getChannelHost();
		chunkChannelHost = new Jedis(channelHostInfo.getHost(), 
				channelHostInfo.getPort(), Constants.INFINITE_TIMEOUT);
		hostPortTypeMessage = new StringBuilder(machineName).
			append(Constants.SEPARATOR_COLON).
			append(port).append(Constants.SEPARATOR_COLON).append(
					AxiomDistributionType.CR_TYPE1_1.toString()).
					append(Constants.SEPARATOR_RATE);
		keysToAdd = new ArrayList<String>();
	}
	
	@Override
	public void cleanUp() {
		if(resultStore != null)
			resultStore.disconnect();
		if(scoreDB != null)
			scoreDB.disconnect();
		if(chunkChannelHost != null)
			chunkChannelHost.disconnect();
	}
	
	private boolean applyRule(String axiomKey, 
			Set<String> axiomValue) throws Exception {
		List<String> axiomValueList = new ArrayList<String>(axiomValue);
		double score = Util.getScore(scoreDB, axiomKey);
		String exclScore = "(" + score;
		axiomValueList.add(exclScore);			
//		System.out.println(axiomValueList.size() + "   " + exclScore);
		// TODO: Try pipelined eval			
//		long startTime = System.nanoTime();
		ArrayList<String> nextMinScoreList = 
						(ArrayList<String>) resultStore.eval(
								scriptSingleConcept, 
								Collections.singletonList(axiomKey), 
								axiomValueList);
//		long endTime = System.nanoTime();
//		totalDiffTime = totalDiffTime + (endTime - startTime);
		double nextMinScore = Double.parseDouble(nextMinScoreList.get(0));		
		Util.setScore(scoreDB, axiomKey, nextMinScore);				
		Long numUpdates = Long.parseLong(nextMinScoreList.get(1));
//		System.out.println("NumUpdates: " + numUpdates);
		if(numUpdates > 0) {
			for(String superClass : axiomValue)
				keysToAdd.add(superClass);
		}			
		return (numUpdates > 0)?true:false;
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
			System.out.println("T11: Time taken to read axiomKey values from " + 
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
		responseList.clear();
		
		if(isInstrumentationEnabled)
			System.out.println("T11: Time taken to applyRule() on the chunk: " + 
					Util.getElapsedTimeSecs(applyRuleStartTime));

		if(!keysToAdd.isEmpty()) {
			
			long addUpdatedKeysLocallyTime = 0;
			if(isInstrumentationEnabled)
				addUpdatedKeysLocallyTime = System.nanoTime();
			
			//add owl:Thing also to keys which were updated. Since it was 
			//updated during initialization itself. Adding it once is sufficient
			if(currentIncrement >= 0) 
				keysToAdd.add(Util.getPackedID(
						Constants.TOP_ID, EntityType.CLASS));
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
				System.out.println("T11: Time taken to add updated keys locally: " + 
						Util.getElapsedTimeSecs(addUpdatedKeysLocallyTime));
			
		}
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
}
