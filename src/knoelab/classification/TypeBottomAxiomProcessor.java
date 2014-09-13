package knoelab.classification;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import knoelab.classification.base.TypeBottomAxiomProcessorBase;
import knoelab.classification.controller.CommunicationHandler;
import knoelab.classification.init.AxiomDistributionType;
import knoelab.classification.misc.Constants;
import knoelab.classification.misc.ScriptsCollection;
import knoelab.classification.misc.Util;
import knoelab.classification.worksteal.ProgressMessageHandler;
import knoelab.classification.worksteal.WorkStealer;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Tuple;

/**
 * This class implements Completion Rule 5 from "Pushing the EL 
 * Envelope Further" TechReport. This rule is as follows:
 * If (C,D) \in R(r) and \bottom \in S(D) then add \bottom to S(C)  
 * 
 * @author Raghava
 *
 */
public class TypeBottomAxiomProcessor extends TypeBottomAxiomProcessorBase {
	
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

	public TypeBottomAxiomProcessor(String machineName, int port) {
		super(machineName, port);
		this.machineName = machineName;
		allChannelsKey = propertyFileHandler.getAllChannelsKey();
		communicationHandler = new CommunicationHandler(
				localStore.smembers(allChannelsKey));
		channel = localStore.get(propertyFileHandler.getChannelKey());
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
			System.out.println("TBot: Time taken for init: " + 
					Util.getElapsedTimeSecs(initStartTime));	
		
		double totalChunks;
		double chunkCount;
		double progress;
		boolean continueProcessing = false;
		int chunkSize = propertyFileHandler.getChunkSize();
		WorkStealer workStealer = new WorkStealer();
		try {
			boolean nextIteration = true;
			int iterationCount = 1;
			double currentMinScore = Constants.INIT_SCORE;
			String scoreStr = scoreDB.get(Constants.SCORE1);
			if(scoreStr != null)
				currentMinScore = Double.parseDouble(scoreStr);
			//currentIncrement is only required for WorkStealer method
			//Other types might require this in their call to processOneWorkChunk()
			String currentIncStr = localStore.get(Constants.CURRENT_INCREMENT);
			double currentIncrement = 0;
			if(currentIncStr != null)
				currentIncrement = Double.parseDouble(currentIncStr);
			
			/* If only the newly added elements to bottomConceptID are considered
			 * then I think some inferences go missing in CR6 -- earlier checked
			 * elements might have a corresponding 'Y' now in CR6 nodes.
			 * S(bot) = {A,B,C}. Initially A not found on CR6 nodes. But later at
			 * some point, there could be a corresponding A on CR6? Do as in T32
			 * or retrieve all of S(bot) everytime?
			 */
			
			while(nextIteration) {
				
				long nextIterKeysStartTime = 0;
//				if(isInstrumentationEnabled)
					nextIterKeysStartTime = System.nanoTime();
				
				System.out.println("Starting iteration-" + iterationCount);
				//skipping use of currentMinScore here, see above comment
				
				long allKeysStartTime = 0;
				if(isInstrumentationEnabled)
					allKeysStartTime = System.nanoTime();
				
				Set<Tuple> elementScores = resultStore.zrangeByScoreWithScores(
											bottomConceptID, Double.NEGATIVE_INFINITY, 
											Double.POSITIVE_INFINITY);
				
				if(isInstrumentationEnabled)
					System.out.println("TBot: Time taken to read U(Bot) keys " +
							"from resultStore: " + 
						Util.getElapsedTimeSecs(allKeysStartTime));
				
				System.out.println("Keys: " + elementScores.size());
				//calculate total chunks and get current chunk. 			
				totalChunks = Math.ceil((double)elementScores.size()/chunkSize);
				localStore.set(Constants.TOTAL_CHUNKS, Double.toString(totalChunks));
				//copy keySet to chunkKeys
				Pipeline p = localStore.pipelined();
				for(Tuple s : elementScores) {
					p.zadd(Constants.CHUNK_KEYS, s.getScore(), s.getElement());
//					if(s.getScore() > currentMinScore)
//						currentMinScore = s.getScore();
				}
				p.sync();
				if(!elementScores.isEmpty())
					currentMinScore += Constants.SCORE_INCREMENT;
				chunkCount = 0;
				
				if(isInstrumentationEnabled)
					System.out.println("TBot: Time taken to setup " +
							"iteration: " + Util.getElapsedTimeSecs(
									nextIterKeysStartTime));
				
				if(elementScores.isEmpty()) {
					progress = 1.0;
					sendProgressMessage(progress, iterationCount);
					continueProcessing = false;	
				}
				else {
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
						
						if(isInstrumentationEnabled)
							System.out.println("TBot: Time taken to execute " +
									"decrAndGetChunk script: " + 
									Util.getElapsedTimeSecs(readChunkStartTime));
						
						chunkCount = Integer.parseInt(result.remove(0));
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
									result, null, null, 
									null, null);
							continueProcessing = continueProcessing || status;	
							
							if(isInstrumentationEnabled)
								System.out.println("TBot: Time taken for one chunk: " + 
										Util.getElapsedTimeSecs(oneChunkStartTime));
						}
					}
				}
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
						System.out.println("TBot: Time taken in waiting for " +
								"stealers: " + Util.getElapsedTimeSecs(
										stealerStartTime));
				}
				
				long workStealingStartTime = 0;
//				if(isInstrumentationEnabled)
					workStealingStartTime = System.nanoTime();
				
				//wait till all nodes send in their progress messages
				progressMsgLatch.await();
				workStealer.checkAndStealWork(progressMessageHandler, 
						currentIncrement, iterationCount);
				
//				if(isInstrumentationEnabled)
					System.out.println("TBot: Time spent in helping busy " +
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
					System.out.println("TBot: Time spent on blocking wait: " + 
							Util.getElapsedTimeSecs(blockingWaitStartTime));
				
				System.out.println("nextIteration? " + nextIteration);
				
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
					System.out.println("TBot: Time taken for iteration: " + 
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
			scoreDB.set(Constants.SCORE1, Double.toString(currentMinScore));
		}
		finally {
			localStore.del(Constants.NUM_JOBS);
			localStore.incr(Constants.CURRENT_INCREMENT);
/*			List<String> types = new ArrayList<String>();
			for(AxiomDistributionType type : AxiomDistributionType.values())
				types.add(type.toString());
			localStore.del(types.toArray(new String[0]));
*/			
			communicationHandler.disconnect();
			cleanUp();
		}
	}
}
