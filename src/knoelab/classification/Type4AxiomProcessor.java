package knoelab.classification;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import knoelab.classification.base.Type4AxiomProcessorBase;
import knoelab.classification.controller.CommunicationHandler;
import knoelab.classification.init.AxiomDistributionType;
import knoelab.classification.misc.Constants;
import knoelab.classification.misc.HostInfo;
import knoelab.classification.misc.ScriptsCollection;
import knoelab.classification.misc.Util;
import knoelab.classification.pipeline.PipelineManager;
import knoelab.classification.worksteal.ProgressMessageHandler;
import knoelab.classification.worksteal.WorkStealer;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Tuple;


/**
 * Implements completion rule 4 i.e. Type-4
 * axioms. They belong to the normal forms r < s.
 * 
 * @author Raghava
 *
 */
public class Type4AxiomProcessor extends Type4AxiomProcessorBase {

	private String localKeys;
	private Jedis localStore;
	private HostInfo localHostInfo;
	private CommunicationHandler communicationHandler;
	private String machineName;
	private String channel;
	private String allChannelsKey;
	private ExecutorService executorService;
	private ProgressMessageHandler progressMessageHandler;
	private CountDownLatch waitLatch;
	private CountDownLatch barrierSynch;
	private CountDownLatch progressMsgLatch;
	private boolean isWorkStealingEnabled;
	
	public Type4AxiomProcessor(String machineName, int localHostPort) {
		super(machineName, localHostPort);
		this.machineName = machineName;
		localHostInfo = propertyFileHandler.getLocalHostInfo();
		localHostInfo.setPort(localHostPort);
		localStore = new Jedis(localHostInfo.getHost(), 
					localHostInfo.getPort(), Constants.INFINITE_TIMEOUT);
		localKeys = propertyFileHandler.getLocalKeys();
		channel = localStore.get(propertyFileHandler.getChannelKey());		
		allChannelsKey = propertyFileHandler.getAllChannelsKey();
		communicationHandler = new CommunicationHandler(
				localStore.smembers(allChannelsKey));
		isWorkStealingEnabled = propertyFileHandler.isWorkStealingEnabled();
		if(isWorkStealingEnabled) {
			waitLatch = new CountDownLatch(1);
			barrierSynch = new CountDownLatch(1);
			executorService = Executors.newSingleThreadExecutor();
			progressMsgLatch = new CountDownLatch(1);
			progressMessageHandler = new ProgressMessageHandler(waitLatch, 
					barrierSynch, progressMsgLatch);
			executorService.execute(progressMessageHandler);
		}
	}
	
	public void processRules() throws Exception { 
		if(isWorkStealingEnabled) {
			//wait till the local ProgressMsgHandler is ready
			barrierSynch.await();
			//let all progressMessageHandlers become active; if not msgs could be lost
			communicationHandler.broadcastAndWaitForACK(channel);
			//now all the ProgressMsgHandlers are ready
		}
		
		if(isInstrumentationEnabled)
			System.out.println("T4: Time taken for init: " + 
					Util.getElapsedTimeSecs(initStartTime));
		
		PipelineManager localHostPReader = new PipelineManager(
									Collections.singletonList(localHostInfo), 
									propertyFileHandler.getPipelineQueueSize());
		double score = Constants.INIT_SCORE;
		String scoreStr = scoreDB.get(Constants.SCORE1);
		if(scoreStr != null)
			score = Double.parseDouble(scoreStr);
		int iterationCount = 1;
		String currentIncStr = localStore.get(Constants.CURRENT_INCREMENT);
		double currentIncrement = 0;
		if(currentIncStr != null)
			currentIncrement = Double.parseDouble(currentIncStr);
		
		try {		
			boolean continueProcessing = false;
			boolean nextIteration = true;
			int chunkSize = propertyFileHandler.getChunkSize();
			double totalChunks;
			double chunkCount;
			double progress;
			WorkStealer workStealer = new WorkStealer();
			
			while(nextIteration) {	
				
				long nextIterKeysStartTime = 0;
//				if(isInstrumentationEnabled)
					nextIterKeysStartTime = System.nanoTime();
				
				System.out.println("Starting iteration-" + iterationCount);
//				localStore.select(1);
				
				long allKeysStartTime = 0;
				if(isInstrumentationEnabled)
					allKeysStartTime = System.nanoTime();
				
				Set<Tuple> keyScoreSet = localStore.zrangeByScoreWithScores(
								localKeys, score, Double.POSITIVE_INFINITY);
				
				if(isInstrumentationEnabled)
					System.out.println("T4: Time taken to read keys: " + 
						Util.getElapsedTimeSecs(allKeysStartTime));
				
				System.out.println("\nAxioms to process: " + 
						keyScoreSet.size());
				//calculate total chunks and get current chunk. 			
				totalChunks = Math.ceil((double)keyScoreSet.size()/chunkSize);
				localStore.set(Constants.TOTAL_CHUNKS, Double.toString(totalChunks));
				//copy keySet to chunkKeys
				Pipeline p = localStore.pipelined();
				for(Tuple s : keyScoreSet) {
					p.zadd(Constants.CHUNK_KEYS, s.getScore(), s.getElement());
					if(s.getScore() > score)
						score = s.getScore();
				}
				p.sync();
				if(!keyScoreSet.isEmpty())
					score += Constants.SCORE_INCREMENT;
				chunkCount = 0;
				
				if(isInstrumentationEnabled)
					System.out.println("T4: Time taken to setup " +
							"iteration: " + Util.getElapsedTimeSecs(
									nextIterKeysStartTime));
				
				if(keyScoreSet.isEmpty()) {
					progress = 1.0;
					sendProgressMessage(progress, iterationCount);
					continueProcessing = false;	
				}			
				else {
					keyScoreSet.clear();
//					localStore.select(0);
					while(chunkCount < totalChunks) {
						//Transaction: Read chunkSize keys, increment chunkCount 
						//			   and delete the chunk
						
						long readChunkStartTime = 0;
						if(isInstrumentationEnabled)
							readChunkStartTime = System.nanoTime();
						
						ArrayList<String> result = 
								(ArrayList<String>) localStore.eval(
										ScriptsCollection.decrAndGetChunk, 
								Collections.singletonList(Constants.CHUNK_KEYS), 
								Collections.singletonList(String.valueOf(chunkSize)));
						chunkCount = Integer.parseInt(result.remove(0));
						
						if(isInstrumentationEnabled)
							System.out.println("T4: Time taken to execute " +
									"decrAndGetChunk script: " + 
									Util.getElapsedTimeSecs(readChunkStartTime));
						
//						System.out.println("In T4-processRules(); " +
//								"chunkCount: " + chunkCount);
						if(chunkCount == -1) {
							//all chunks are processed
							progress = 1.0;
							sendProgressMessage(progress, iterationCount);
							break;
						}
						else {
							progress = 
								chunkCount/(double)totalChunks;
							sendProgressMessage(progress, iterationCount);
							
							long oneChunkStartTime = 0;
							if(isInstrumentationEnabled)
								oneChunkStartTime = System.nanoTime();
							
							boolean status = processOneWorkChunk(
									result, 
									localHostPReader, localHostInfo, 
									null, null);
							continueProcessing = continueProcessing || status;
							
							if(isInstrumentationEnabled)
								System.out.println("T4: Time taken for one chunk: " + 
										Util.getElapsedTimeSecs(oneChunkStartTime));
						}
					}
//					localStore.select(0);
				if(isWorkStealingEnabled) {	
					//check whether this process is a stealer or not
					String isStealerStr = localStore.get(Constants.STEALER_BOOLEAN);
					if(isStealerStr != null) {
						//This is not a stealer but a stealee (one from which work is stealed).
						//Check and wait till all stealers finish their work
						
						long stealerStartTime = 0;
						if(isInstrumentationEnabled)
							stealerStartTime = System.nanoTime();
						
						localStore.blpop(0, Constants.STEALERS_WAIT);
						Set<String> stealersStatus = localStore.smembers(
								Constants.STEALERS_STATUS);
						for(String status : stealersStatus) {
							if(status.equals("1")) {
								continueProcessing = true;
								break;
							}
						}
						
						if(isInstrumentationEnabled)
							System.out.println("T4: Time taken in waiting for " +
									"stealers: " + Util.getElapsedTimeSecs(
											stealerStartTime));
					}
				}
				}
				if(isWorkStealingEnabled) {
					//wait till all nodes send in their progress messages
					long workStealingStartTime = 0;
//					if(isInstrumentationEnabled)
						workStealingStartTime = System.nanoTime();
					
					progressMsgLatch.await();
					workStealer.checkAndStealWork(progressMessageHandler, 
							currentIncrement, iterationCount);
					
//					if(isInstrumentationEnabled)
						System.out.println("T4: Time spent in helping busy " +
								"nodes: " + Util.getElapsedTimeSecs(
										workStealingStartTime));
				}
				Util.broadcastMessage(communicationHandler, machineName, 
						continueProcessing, iterationCount);
				
				long blockingWaitStartTime = 0;
//				if(isInstrumentationEnabled)
					blockingWaitStartTime = System.nanoTime();
				
				nextIteration = communicationHandler.blockingWaitAndGetStatus(
									channel, iterationCount);
				
//				if(isInstrumentationEnabled)
					System.out.println("T4: Time spent on blocking wait: " + 
							Util.getElapsedTimeSecs(blockingWaitStartTime));
				
				System.out.println("nextIteration? " + nextIteration);
				continueProcessing = false;
				
				//Clear/Del the keys related to chunks/steal for next iteration
				localStore.del(Constants.STEALER_BOOLEAN, 
						Constants.STEALERS_STATUS, Constants.CHUNK_KEYS,
						Constants.CHUNK_COUNT);
				if(isWorkStealingEnabled) {
					progressMsgLatch = new CountDownLatch(1);
					progressMessageHandler.resetLatchAndClearMessages(
							progressMsgLatch, iterationCount);
				}
				iterationCount++;
				
//				if(isInstrumentationEnabled)
					System.out.println("T4: Time taken for iteration: " + 
							Util.getElapsedTimeSecs(nextIterKeysStartTime));
				System.out.println("\n");
			}
			if(isWorkStealingEnabled) {
				progressMessageHandler.unsubscribe();
				executorService.shutdown();
				waitLatch.await();
				if(!executorService.isTerminated())
					executorService.awaitTermination(5, TimeUnit.SECONDS);
			}
			//to handle incremental data
			scoreDB.set(Constants.SCORE1, Double.toString(score));
		}
		finally {
//			localStore.select(0);
			localStore.del(Constants.NUM_JOBS);
			localStore.incr(Constants.CURRENT_INCREMENT);
/*			
			List<String> types = new ArrayList<String>();
			for(AxiomDistributionType type : AxiomDistributionType.values())
				types.add(type.toString());
			localStore.del(types.toArray(new String[0]));
*/			
			localHostPReader.closeAll();
			communicationHandler.disconnect();
			localStore.disconnect();
			cleanUp();
		}
	}
	
}
