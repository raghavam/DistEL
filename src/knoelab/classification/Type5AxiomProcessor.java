package knoelab.classification;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import knoelab.classification.base.Type5AxiomProcessorBase;
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
import redis.clients.jedis.Response;

/**
 * Implements role chain axioms, r o s < t i.e.,
 * completion rule R5.
 * 
 * @author Raghava
 *
 */
public class Type5AxiomProcessor extends Type5AxiomProcessorBase {

	private Jedis localStore;
	private HostInfo localHostInfo;
	private CommunicationHandler communicationHandler;
	private String machineName;
	private String channel;
//	private Jedis scoreDB1;
//	private Jedis scoreDB2;
//	private final int DB5 = 5;
//	private final int DB6 = 6;
	private String allChannelsKey;
	private ExecutorService executorService;
	private ProgressMessageHandler progressMessageHandler;
	private CountDownLatch waitLatch;
	private CountDownLatch barrierSynch;
	private CountDownLatch progressMsgLatch;
	private boolean isWorkStealingEnabled;
	
	public Type5AxiomProcessor(String machineName, int localHostPort) {
		super(machineName, localHostPort);
		this.machineName = machineName;
		
/*		scoreDB1 = new Jedis(localHostInfo.getHost(), 
				localHostInfo.getPort(), Constants.INFINITE_TIMEOUT);
		scoreDB1.select(DB5);
		scoreDB2 = new Jedis(localHostInfo.getHost(), 
				localHostInfo.getPort(), Constants.INFINITE_TIMEOUT);
		scoreDB2.select(DB6);
*/		
		localHostInfo = propertyFileHandler.getLocalHostInfo();
		localHostInfo.setPort(localHostPort);
		localStore = new Jedis(localHostInfo.getHost(), 
					localHostInfo.getPort(), Constants.INFINITE_TIMEOUT);
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
			System.out.println("T5: Time taken for init: " + 
					Util.getElapsedTimeSecs(initStartTime));	
		
		PipelineManager localHostPReader = new PipelineManager(
				Collections.singletonList(localHostInfo), 
				propertyFileHandler.getPipelineQueueSize());
				new ArrayList<Response<Set<String>>>();
		int iterationCount = 1;
		try {
			boolean continueProcessing = false;
			boolean nextIteration = true;
			String localKeys = propertyFileHandler.getLocalKeys();
			int chunkSize = propertyFileHandler.getChunkSize();
			double totalChunks;
			double chunkCount;
			double progress;
			WorkStealer workStealer = new WorkStealer();
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
				// Phase-1; match Yr from DB-1 to Xr from DB-4 
				localStore.select(1);
				
				long allKeysStartTime = 0;
				if(isInstrumentationEnabled)
					allKeysStartTime = System.nanoTime();
				
				Set<String> keySet = localStore.smembers(localKeys);
				
				if(isInstrumentationEnabled)
					System.out.println("T5: Time taken to read all keys: " + 
						Util.getElapsedTimeSecs(allKeysStartTime));
				
				System.out.println("Axioms to process: " + 
						keySet.size());
				//calculate total chunks and get current chunk. 			
				totalChunks = Math.ceil((double)keySet.size()/chunkSize);
				localStore.select(0);
				localStore.set(Constants.TOTAL_CHUNKS, Double.toString(totalChunks));
				System.out.println("Total Chunks: " + totalChunks);
				//copy keySet to chunkKeys
				Pipeline p = localStore.pipelined();
				for(String s : keySet)
					p.zadd(Constants.CHUNK_KEYS, Constants.INIT_SCORE, s);
				p.sync();
				chunkCount = 0;
				
				if(isInstrumentationEnabled)
					System.out.println("T5: Time taken to setup " +
							"iteration: " + Util.getElapsedTimeSecs(
									nextIterKeysStartTime));
				
				if(keySet.isEmpty()) {
					progress = 1.0;
					sendProgressMessage(progress, iterationCount);
					continueProcessing = false;	
				}
				else {
					while(chunkCount < totalChunks) {
						//Transaction: Read chunkSize keys, increment chunkCount 
						//			   and delete the chunk
						localStore.select(0);
						
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
							System.out.println("T5: Time taken to execute " +
									"decrAndGetChunk script: " + 
									Util.getElapsedTimeSecs(readChunkStartTime));
						
//						System.out.println("T5; Chunk Count: " + chunkCount);
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
									null, localStore);
							continueProcessing = continueProcessing || status;	
							
							if(isInstrumentationEnabled)
								System.out.println("T5: Time taken for one chunk: " + 
										Util.getElapsedTimeSecs(oneChunkStartTime));
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
							System.out.println("T5: Time taken in waiting for " +
									"stealers: " + Util.getElapsedTimeSecs(
											stealerStartTime));
					}
				}
				}
				localStore.select(0);
			if(isWorkStealingEnabled) {	
				
				long workStealingStartTime = 0;
				if(isInstrumentationEnabled)
					workStealingStartTime = System.nanoTime();
				
				//wait till all nodes send in their progress messages
				progressMsgLatch.await();
				workStealer.checkAndStealWork(progressMessageHandler, 
						currentIncrement, iterationCount);
				
				if(isInstrumentationEnabled)
					System.out.println("T5: Time spent in helping busy " +
							"nodes: " + Util.getElapsedTimeSecs(
									workStealingStartTime));
			}
				Util.broadcastMessage(communicationHandler, machineName, 
						continueProcessing, iterationCount);
				
				long blockingWaitStartTime = 0;
				if(isInstrumentationEnabled)
					blockingWaitStartTime = System.nanoTime();
				
				nextIteration = communicationHandler.blockingWaitAndGetStatus(
									channel, iterationCount);
				
				if(isInstrumentationEnabled)
					System.out.println("T5: Time spent on blocking wait: " + 
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
				
				if(isInstrumentationEnabled)
					System.out.println("T5: Time taken for iteration: " + 
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
		}
		finally {
			localStore.select(0);
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
//			scoreDB1.disconnect();
//			scoreDB2.disconnect();
			localStore.disconnect();
			cleanUp();
		}
	}
	
}
