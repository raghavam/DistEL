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
import knoelab.classification.misc.Util;
import knoelab.classification.pipeline.PipelineManager;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Response;
import redis.clients.jedis.Tuple;

public class Type3_2AxiomProcessorBase implements AxiomProcessor {

	protected PropertyFileHandler propertyFileHandler;
	protected Jedis roleStore;
	protected Jedis localStore;
	protected Jedis scoreDB;
	private Jedis resultStore;
	private String insertionScript;
	private Jedis chunkChannelHost;
	private StringBuilder hostPortTypeMessage;
	private HostInfo resultHostInfo;
	private PipelineManager resultStorePipelineMgr;
	private List<Response<String>> evalResponseList;
	
	protected boolean isInstrumentationEnabled;
	protected long initStartTime;
	
	public Type3_2AxiomProcessorBase(String machineName, int port) {
		propertyFileHandler = PropertyFileHandler.getInstance();
		
		isInstrumentationEnabled = 
				propertyFileHandler.isInstrumentationEnabled();
		if(isInstrumentationEnabled)
			initStartTime = System.nanoTime();
		
		HostInfo channelHostInfo = propertyFileHandler.getChannelHost();
		chunkChannelHost = new Jedis(channelHostInfo.getHost(), 
				channelHostInfo.getPort(), Constants.INFINITE_TIMEOUT);
		hostPortTypeMessage = new StringBuilder(machineName).
			append(Constants.SEPARATOR_COLON).
			append(port).append(Constants.SEPARATOR_COLON).append(
					AxiomDistributionType.CR_TYPE3_2.toString()).
					append(Constants.SEPARATOR_RATE);
		localStore = new Jedis(machineName, port, Constants.INFINITE_TIMEOUT);
		roleStore = new Jedis(machineName, port, Constants.INFINITE_TIMEOUT);
		roleStore.select(AxiomDB.ROLE_DB.getDBIndex());
		scoreDB = new Jedis(machineName, port, Constants.INFINITE_TIMEOUT);
		scoreDB.select(AxiomDB.SCORE_DB.getDBIndex());
		resultHostInfo = propertyFileHandler.getResultNode();
		resultStore = new Jedis(resultHostInfo.getHost(), 
							resultHostInfo.getPort(), Constants.INFINITE_TIMEOUT);
		resultStorePipelineMgr = new PipelineManager(
				Collections.singletonList(resultHostInfo), 
				propertyFileHandler.getPipelineQueueSize());
		
		// need to connect explicitly for lua scripting - gives a NPE if not done.
		resultStore.connect();
		
		insertionScript = "local unique = 0 " +
		  "local size = redis.call('ZCARD', 'keysUpdated') " +
		  "local keysUpdatedScore " +
		  "local highestEScore " +
		  "local escore " +
		  "local score " +
		  "local ret " +
		  "if(size == 0) then " +
		  	"keysUpdatedScore = 1.0 " +
		  "else " +
		  	"highestEScore = redis.call('ZRANGE', 'keysUpdated', " +
		  							"-1, -1, 'WITHSCORES') " +
		  	"keysUpdatedScore = highestEScore[2] + " + 
		  							Constants.SCORE_INCREMENT + " " +
		  "end " + 
		  "for index1,value1 in pairs(KEYS) do " +
			"escore = redis.call('ZRANGE', value1, -1, -1, 'WITHSCORES') " +
			"score = escore[2] + " + Constants.SCORE_INCREMENT + " " +
			"for index2,value2 in pairs(ARGV) do " +
				"if(not redis.call('ZSCORE', value1, value2)) then  " +
					"ret = redis.call('ZADD', value1, score, value2) " +
					"if(ret > 0) then " +
						"redis.call('ZADD', 'keysUpdated', " +
										"keysUpdatedScore, value1) " +
					"end " +
					"unique = unique + ret " +
				"end " +
			"end " +
		  "end " +
		  "return tostring(unique) ";
	}
	
	@Override
	public boolean processOneWorkChunk(List<String> chunkKeys,
			PipelineManager pipelinedReader, HostInfo keysHostInfo,
			Double currentIncrement, Jedis keyStore) throws Exception {
		int i = 0;
		boolean continueProcessing = false;
		boolean axiomUpdate;
		boolean whichDB;
		AxiomDB axiomDB;
		double nextMinScore;
		if(currentIncrement > 0) {
			whichDB = true;
			axiomDB = AxiomDB.NON_ROLE_DB;
		}
		else {
			whichDB = false;
			axiomDB = AxiomDB.ROLE_DB;
		}
		
		long keysReadStartTime = 0;
		if(isInstrumentationEnabled)
			keysReadStartTime = System.nanoTime();
		
		for(String axiomKey : chunkKeys) {
			nextMinScore = getScore(axiomKey, whichDB);
				
			nextMinScore += Constants.SCORE_INCREMENT;
			// SCORE_INCREMENT has been added to score in order to replicate the 
			// functionality of (score i.e. exclude 'score' in the zrange	
				
			pipelinedReader.pZRangeByScoreWithScores(keysHostInfo, axiomKey, 
					nextMinScore, Double.POSITIVE_INFINITY, axiomDB);
		}
		pipelinedReader.synchAll(axiomDB);
		List<Response<Set<Tuple>>> zrangeResponseList1 = 
			pipelinedReader.getZrangeByScoreWithScoresResponseList();
		
		if(isInstrumentationEnabled)
			System.out.println("T32: Time taken to read axiomKey values from " + 
					keysHostInfo.toString() + ": " + Util.getElapsedTimeSecs(
							keysReadStartTime));
		
		long applyRuleStartTime = 0;
		if(isInstrumentationEnabled)
			applyRuleStartTime = System.nanoTime();
		
		for(String axiomKey : chunkKeys) {
			Set<Tuple> response1 = zrangeResponseList1.get(i).get();
			if(!response1.isEmpty()) 
				applyRule(axiomKey, response1, whichDB);
			i++;
		}
		
		resultStorePipelineMgr.synchAndCloseAll(AxiomDB.NON_ROLE_DB);
		evalResponseList = new ArrayList<Response<String>>(
							resultStorePipelineMgr.getEvalResponseList());
		resultStorePipelineMgr.resetSynchResponse();
		
		for(Response<String> response : evalResponseList) {
			Long numUpdates = Long.parseLong(response.get());
			axiomUpdate = (numUpdates > 0)?true:false;
			continueProcessing = continueProcessing || axiomUpdate;
		}
		
		if(isInstrumentationEnabled)
			System.out.println("T32: Time taken to applyRule() on the chunk: " + 
					Util.getElapsedTimeSecs(applyRuleStartTime));
		
//		if(!chunkKeys.isEmpty())
//			totalDiffTime = totalDiffTime/chunkKeys.size();
//		System.out.println("ResultStore.eval(): " + (totalDiffTime/Constants.NANO));
		if(i != zrangeResponseList1.size())
			throw new Exception("i should be same as list size");
		pipelinedReader.resetSynchResponse();
		return continueProcessing;
	}
	
	/**
	 * 
	 * @param axiomKey
	 * @param axiomValue
	 * @param whichDB true represents DB-0 and false represents DB-1 
	 */
	private void applyRule(String axiomKey, Set<Tuple> axiomValueScore, 
			boolean whichDB) throws Exception {
		Set<String> rolePairs = null;
		if(whichDB)
			rolePairs = roleStore.zrange(axiomKey, 0, -1);
		else
			rolePairs = localStore.zrange(axiomKey, 0, -1);	
		
		if(rolePairs.isEmpty())
			return;
		
		List<String> axiomValue = new ArrayList<String>(axiomValueScore.size());
		double score = 0;
		for(Tuple t : axiomValueScore) {
			axiomValue.add(t.getElement());
			if(t.getScore() > score)
				score = t.getScore();
		}
		
//		Long numUpdates = new Long(0);
		if(whichDB) {		
			if(!axiomValue.isEmpty() && !rolePairs.isEmpty()) {
//				numUpdates = (Long)resultStore.eval(insertionScript, axiomValue, 
//						new ArrayList<String>(rolePairs));
				resultStorePipelineMgr.peval(resultHostInfo, 
						insertionScript, axiomValue, 
						new ArrayList<String>(rolePairs), 
						AxiomDB.NON_ROLE_DB);
			}
		}
		else {
			if(!axiomValue.isEmpty() && !rolePairs.isEmpty()) {
//				numUpdates = (Long)resultStore.eval(insertionScript, 
//						new ArrayList<String>(rolePairs), axiomValue);
				resultStorePipelineMgr.peval(resultHostInfo, 
						insertionScript, new ArrayList<String>(rolePairs), 
						axiomValue, AxiomDB.NON_ROLE_DB);
			}
		}
		setScore(axiomKey, score, whichDB);
//		System.out.println("NumUpdates: " + numUpdates);
//		return (numUpdates > 0)?true:false;
	}
	
	private void setScore(String axiomKey, double score, boolean whichDB) {
		if(whichDB)
			scoreDB.hset(axiomKey, Constants.FIELD1, Double.toString(score));
		else
			scoreDB.hset(axiomKey, Constants.FIELD2, Double.toString(score));
	}
	
	private double getScore(String axiomKey, boolean whichDB) {
		String score;
		if(whichDB) 
			score = scoreDB.hget(axiomKey, Constants.FIELD1);
		else
			score = scoreDB.hget(axiomKey, Constants.FIELD2);
		if(score == null)
			score = "0";		
		return Double.parseDouble(score);
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
		if(localStore != null)
			localStore.disconnect();
		if(roleStore != null)
			roleStore.disconnect();
		if(scoreDB != null)
			scoreDB.disconnect();
		if(resultStore != null)
			resultStore.disconnect();
	}
}
