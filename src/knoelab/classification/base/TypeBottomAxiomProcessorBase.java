package knoelab.classification.base;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
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
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;
import redis.clients.jedis.Tuple;

public class TypeBottomAxiomProcessorBase implements AxiomProcessor {
	
	protected String bottomConceptID;
	protected Jedis scoreDB;
	protected PropertyFileHandler propertyFileHandler;
	private Jedis chunkChannelHost;
	private StringBuilder hostPortTypeMessage;
	protected Jedis resultStore;
	protected Jedis localStore;
	
	protected boolean isInstrumentationEnabled;
	protected long initStartTime;
	
	public TypeBottomAxiomProcessorBase(String machineName, int port) {
		propertyFileHandler = PropertyFileHandler.getInstance();
		
		isInstrumentationEnabled = 
				propertyFileHandler.isInstrumentationEnabled();
		if(isInstrumentationEnabled)
			initStartTime = System.nanoTime();
		
		bottomConceptID = Util.getPackedID(Constants.BOTTOM_ID, 
				EntityType.CLASS);
		scoreDB = new Jedis(machineName, port, Constants.INFINITE_TIMEOUT);
		scoreDB.select(AxiomDB.SCORE_DB.getDBIndex());
		HostInfo channelHostInfo = propertyFileHandler.getChannelHost();
		chunkChannelHost = new Jedis(channelHostInfo.getHost(), 
				channelHostInfo.getPort(), Constants.INFINITE_TIMEOUT);
		hostPortTypeMessage = new StringBuilder(machineName).
			append(Constants.SEPARATOR_COLON).
			append(port).append(Constants.SEPARATOR_COLON).append(
					AxiomDistributionType.CR_TYPE_BOTTOM.toString()).
					append(Constants.SEPARATOR_RATE);
		HostInfo resultHostInfo = propertyFileHandler.getResultNode();
		resultStore = new Jedis(resultHostInfo.getHost(), 
				resultHostInfo.getPort(), Constants.INFINITE_TIMEOUT);
		localStore = new Jedis(machineName, port, Constants.INFINITE_TIMEOUT);
	}

	@Override
	public boolean processOneWorkChunk(List<String> chunkKeys,
			PipelineManager pipelinedReader, HostInfo keysHostInfo,
			Double currentIncrement, Jedis keyStore) throws Exception {
		double keyScore = 0;
		Pipeline p = localStore.pipelined();
		List<Response<Set<Tuple>>> responseList = 
			new ArrayList<Response<Set<Tuple>>>(chunkKeys.size());
		
		long keysReadStartTime = 0;
		if(isInstrumentationEnabled)
			keysReadStartTime = System.nanoTime();
		
		for(String axiomKey : chunkKeys) {
			keyScore = Util.getScore(scoreDB, axiomKey);
			responseList.add(p.zrangeByScoreWithScores(axiomKey, 
					keyScore + Constants.SCORE_INCREMENT, 
					Double.POSITIVE_INFINITY));
		}
		p.sync();
		
		if(isInstrumentationEnabled)
			System.out.println("TBot: Time taken to read axiomKey values: " + 
					Util.getElapsedTimeSecs(
							keysReadStartTime));
		
		int index = 0;
		Set<String> elementsToAdd = new HashSet<String>();
		for(String axiomKey : chunkKeys) {
			Set<Tuple> localElementScores = responseList.get(index).get();
			for(Tuple t2 : localElementScores) {
				elementsToAdd.add(t2.getElement());
				if(t2.getScore() > keyScore)
					keyScore = t2.getScore();
			}				
			Util.setScore(scoreDB, axiomKey, keyScore);
			index++;
		}
		responseList.clear();
		List<String> elementsToAddList = new ArrayList<String>(
				elementsToAdd);
		elementsToAdd.clear();
		Long numUpdates = new Long(0);
		if(!elementsToAddList.isEmpty()) {

			long addElementsToBotStartTime = 0;
			if(isInstrumentationEnabled)
				addElementsToBotStartTime = System.nanoTime();
			
			numUpdates = (Long) resultStore.eval(
				ScriptsCollection.insertInBottom, 
				Collections.singletonList(bottomConceptID), 
				elementsToAddList);
			
			if(isInstrumentationEnabled)
				System.out.println("TBot: Time taken for insertInBottom script: " + 
						Util.getElapsedTimeSecs(addElementsToBotStartTime));
			
			elementsToAddList.clear();
		}
		return (numUpdates > 0) ? true : false;
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
		if(resultStore != null)
			resultStore.disconnect();
		if(localStore != null)
			localStore.disconnect();
	}
}
