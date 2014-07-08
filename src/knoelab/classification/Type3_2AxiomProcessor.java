package knoelab.classification;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import knoelab.classification.base.Type3_2AxiomProcessorBase;
import knoelab.classification.controller.CommunicationHandler;
import knoelab.classification.init.AxiomDistributionType;
import knoelab.classification.misc.Constants;
import knoelab.classification.misc.HostInfo;
import knoelab.classification.misc.ScriptsCollection;
import knoelab.classification.misc.Util;
import knoelab.classification.pipeline.PipelineManager;
import knoelab.classification.worksteal.ProgressMessageHandler;
import knoelab.classification.worksteal.WorkStealer;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;
import redis.clients.jedis.Transaction;
import redis.clients.jedis.Tuple;

/**
 * Implements part-2 of completion rule 4 i.e. Type-3
 * axioms. It is split into 2 rules.
 * Type-3_2: if 3r.Y < B and (X,Y) \in R(r) then
 * 			 S(X) = S(X) U {B} 
 * 
 * @author Raghava
 *
 */
public class Type3_2AxiomProcessor extends Type3_2AxiomProcessorBase {

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
	
	public Type3_2AxiomProcessor(String machineName, int port) {
		super(machineName, port);
		this.machineName = machineName;
		localHostInfo = propertyFileHandler.getLocalHostInfo();
		localHostInfo.setPort(port);
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
			System.out.println("T32: Time taken for init: " + 
					Util.getElapsedTimeSecs(initStartTime));	
		
		PipelineManager localHostPReader = new PipelineManager(
				Collections.singletonList(localHostInfo), 
				propertyFileHandler.getPipelineQueueSize());		
		try {
			String localKeys = propertyFileHandler.getLocalKeys();
			int chunkSize = propertyFileHandler.getChunkSize();
			double totalChunks;
			double chunkCount;
			double progress;
			WorkStealer workStealer = new WorkStealer();
			
			boolean continueProcessing = false;
			boolean nextIteration = true;
			int iterationCount = 1;			
			double score1 = getLocalKeysScore(true);
			double score2 = getLocalKeysScore(false);
			//currentIncrement is only required for WorkStealer method
			//Other types might require this in their call to processOneWorkChunk()
			String currentIncStr = localStore.get(Constants.CURRENT_INCREMENT);
			double currentIncrement = 0;
			if(currentIncStr != null)
				currentIncrement = Double.parseDouble(currentIncStr);
			
			while(nextIteration) {		
				
				long nextIterKeysStartTime = 0;
				if(isInstrumentationEnabled)
					nextIterKeysStartTime = System.nanoTime();
				
				System.out.println("\nStarting iteration-" + iterationCount);
				
				long allKeysStartTime = 0;
				if(isInstrumentationEnabled)
					allKeysStartTime = System.nanoTime();
				
				Transaction localTransaction = localStore.multi();
				Response<Set<Tuple>> keySetResponse1 = localTransaction.zrangeByScoreWithScores(
														localKeys, score1, Double.POSITIVE_INFINITY);
				Response<Set<String>> totalKeys1 = localTransaction.zrange(localKeys, 0, -1);
				localTransaction.exec();
				Transaction roleTransaction = roleStore.multi();
				Response<Set<Tuple>> keySetResponse2 = roleTransaction.zrangeByScoreWithScores(
														localKeys, score2, Double.POSITIVE_INFINITY);
				Response<Set<String>> totalKeys2 = roleTransaction.zrange(localKeys, 0, -1);
				roleTransaction.exec();
				
				if(isInstrumentationEnabled)
					System.out.println("T32: Time taken to read all keys " +
							"(part1, part2): " + 
						Util.getElapsedTimeSecs(allKeysStartTime));
								
				if(keySetResponse1.get().isEmpty() && keySetResponse2.get().isEmpty()) {
					System.out.println("\nBoth key sets are empty, so skipping...");
					sendProgressMessage(1.0, iterationCount);
					nextIteration = stealAndGetStatus(workStealer, 
							false, currentIncrement, iterationCount);
					prepareForNextIteration(iterationCount);
					iterationCount++;
					System.out.println("nextIteration? " + nextIteration);	
					if(isInstrumentationEnabled)
						System.out.println("T32: Time taken for iteration: " + 
								Util.getElapsedTimeSecs(nextIterKeysStartTime));
					continue;
				}
				//calculate total chunks and get current chunk. 	
				double numAxioms1 = keySetResponse1.get().size();
				double totalChunks1 = Math.ceil(numAxioms1/chunkSize);
//				System.out.println("Before retrieval; score1: " + score1);
				//copy keySet to chunkKeys
				Pipeline p = localStore.pipelined();
				for(Tuple s : keySetResponse1.get()) {
					p.zadd(Constants.CHUNK_KEYS, s.getScore(), s.getElement());
					if(s.getScore() > score1)
						score1 = s.getScore();
				}
				p.sync();
//				System.out.println("After retrieval; score1: " + score1);
				
				//do the same for part-2
				//calculate total chunks and get current chunk. 
				double numAxioms2 = keySetResponse2.get().size();
				double totalChunks2 = Math.ceil(numAxioms2/chunkSize);
//				System.out.println("Before retrieval; score2: " + score2);
				//copy keySet to chunkKeys
				for(Tuple s : keySetResponse2.get()) {
					p.zadd(Constants.CHUNK_KEYS2, s.getScore(), s.getElement());
					if(s.getScore() > score2)
						score2 = s.getScore();
				}
				p.sync();
//				System.out.println("After retrieval; score2: " + score2);
				
				totalChunks = totalChunks1 + totalChunks2;
				localStore.set(Constants.TOTAL_CHUNKS, Double.toString(totalChunks));
				
				System.out.println("Axioms to process part-1: " + numAxioms1 + 
						"  Total: " + totalKeys1.get().size() + 
						"  Total chunks: " + totalChunks1);
				totalKeys1.get().clear();
				keySetResponse1.get().clear();
				keySetResponse2.get().clear();

				chunkCount = 0;
				
				if(isInstrumentationEnabled)
					System.out.println("T32: Time taken to setup " +
							"iteration: " + Util.getElapsedTimeSecs(
									nextIterKeysStartTime));
				
				// PART-1
				if(totalChunks1 != 0) {
					score1 += Constants.SCORE_INCREMENT;
					while(true) {
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
							System.out.println("T32_1: Time taken to execute " +
									"decrAndGetChunk script: " + 
									Util.getElapsedTimeSecs(readChunkStartTime));
						
//						System.out.println("In T3-2 part1; Chunk count: " + chunkCount);
						if(chunkCount == -1) {
							//all chunks of part-1 are processed 
							//but don't know about part-2. So make progress=0
							progress = 0.0;
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
									1.0, null);
							continueProcessing = continueProcessing || status;	
							
							if(isInstrumentationEnabled)
								System.out.println("T32_1: Time taken for one chunk: " + 
										Util.getElapsedTimeSecs(oneChunkStartTime));
						}
					}
				}
				else 
					sendProgressMessage(0.0, iterationCount);
				
				System.out.println("Axioms to process part-2: " + numAxioms2 + 
						"  Total: " + totalKeys2.get().size() + 
						"  Total chunks: " + totalChunks2);
				totalKeys2.get().clear();
				
				// PART-2
				if(totalChunks2 != 0) {
					score2 += Constants.SCORE_INCREMENT;
					while(true) {
						//Transaction: Read chunkSize keys, increment chunkCount 
						//			   and delete the chunk
						
						long readChunkStartTime = 0;
						if(isInstrumentationEnabled)
							readChunkStartTime = System.nanoTime();
						
						ArrayList<String> result = 
								(ArrayList<String>) localStore.eval(
										ScriptsCollection.decrAndGetChunk, 
								Collections.singletonList(Constants.CHUNK_KEYS2), 
								Collections.singletonList(String.valueOf(chunkSize)));
						chunkCount = Integer.parseInt(result.remove(0));
						
						if(isInstrumentationEnabled)
							System.out.println("T32_2: Time taken to execute " +
									"decrAndGetChunk script: " + 
									Util.getElapsedTimeSecs(readChunkStartTime));
						
//						System.out.println("In T3-2 part2; Chunk count: " + chunkCount);
						if(chunkCount == -1) {
							//all chunks of part-2 are processed
							//but don't know about part-1. So make progress=0
							progress = 0.0;
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
									-1.0, null);
							continueProcessing = continueProcessing || status;	
							
							if(isInstrumentationEnabled)
								System.out.println("T32_2: Time taken for one chunk: " + 
										Util.getElapsedTimeSecs(oneChunkStartTime));
						}
					}
				}
				else 
					sendProgressMessage(0.0, iterationCount);
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
						System.out.println("T32: Time taken in waiting for " +
								"stealers: " + Util.getElapsedTimeSecs(
										stealerStartTime));
				}
			}
				nextIteration = stealAndGetStatus(workStealer, 
						continueProcessing, currentIncrement, iterationCount);	
				
				System.out.println("nextIteration? " + nextIteration);
				continueProcessing = false;
				prepareForNextIteration(iterationCount);
				iterationCount++;
				
				if(isInstrumentationEnabled)
					System.out.println("T32: Time taken for iteration: " + 
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
			setLocalKeysScore(Constants.SCORE1, score1);
			setLocalKeysScore(Constants.SCORE2, score2);
		}
		finally {
			localStore.del(Constants.NUM_JOBS);
			localStore.incr(Constants.CURRENT_INCREMENT);
//			List<String> types = new ArrayList<String>();
//			for(AxiomDistributionType type : AxiomDistributionType.values())
//				types.add(type.toString());
//			localStore.del(types.toArray(new String[0]));
			localHostPReader.closeAll();
			communicationHandler.disconnect();
			cleanUp();
		}
	}	
	
	private void prepareForNextIteration(int iterationCount) {
		//Clear/Del the keys related to chunks/steal for next iteration
		localStore.del(Constants.STEALER_BOOLEAN, 
				Constants.STEALERS_STATUS, Constants.CHUNK_KEYS,
				Constants.CHUNK_KEYS2, Constants.CHUNK_COUNT);
		if(isWorkStealingEnabled) {
			progressMsgLatch = new CountDownLatch(1);
			progressMessageHandler.resetLatchAndClearMessages(
					progressMsgLatch, iterationCount);
		}
	}
	
	private boolean stealAndGetStatus(WorkStealer workStealer, 
			boolean continueProcessing, double currentIncrement, 
			int iterationCount) throws Exception {
		if(isWorkStealingEnabled) {
			
			long workStealingStartTime = 0;
			if(isInstrumentationEnabled)
				workStealingStartTime = System.nanoTime();
			
			//wait till all nodes send in their progress messages
			progressMsgLatch.await();
			workStealer.checkAndStealWork(progressMessageHandler, 
					currentIncrement, iterationCount);
			
			if(isInstrumentationEnabled)
				System.out.println("T32: Time spent in helping busy " +
						"nodes: " + Util.getElapsedTimeSecs(
								workStealingStartTime));
		}		
		Util.broadcastMessage(communicationHandler, machineName, 
				continueProcessing, iterationCount);
		
		long blockingWaitStartTime = 0;
		if(isInstrumentationEnabled)
			blockingWaitStartTime = System.nanoTime();
		
		boolean nextIteration = communicationHandler.blockingWaitAndGetStatus(
							channel, iterationCount);
		
		if(isInstrumentationEnabled)
			System.out.println("T32: Time spent on blocking wait: " + 
					Util.getElapsedTimeSecs(blockingWaitStartTime));
		
		return nextIteration;
	}
	
	private double getLocalKeysScore(boolean isPart1) {
		String score;
		if(isPart1)
			score = scoreDB.get(Constants.SCORE1);
		else
			score = scoreDB.get(Constants.SCORE2);
		if(score == null)
			return Constants.INIT_SCORE;
		else
			return Double.parseDouble(score);
	}
	
	private void setLocalKeysScore(String key, double score) {
		scoreDB.set(key, Double.toString(score));
	}
}
