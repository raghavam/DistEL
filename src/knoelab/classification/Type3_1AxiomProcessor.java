package knoelab.classification;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import knoelab.classification.base.Type3_1AxiomProcessorBase;
import knoelab.classification.controller.CommunicationHandler;
import knoelab.classification.init.AxiomDistributionType;
import knoelab.classification.misc.Constants;
import knoelab.classification.misc.HostInfo;
import knoelab.classification.misc.PropertyFileHandler;
import knoelab.classification.misc.ScriptsCollection;
import knoelab.classification.misc.Util;
import knoelab.classification.pipeline.PipelineManager;
import knoelab.classification.worksteal.ProgressMessageHandler;
import knoelab.classification.worksteal.WorkStealer;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Tuple;

/**
 * Implements part-1 of completion rule 4 i.e. Type-3
 * axioms. It is split into 2 rules.
 * Type-3_1: if 3r.A < B and A \in S(Y) then
 * 			 3r.Y < B
 * 
 * @author Raghava
 *
 */
public class Type3_1AxiomProcessor extends Type3_1AxiomProcessorBase {
	
	private HostInfo localHostInfo;
	private CommunicationHandler communicationHandler;
	private String machineName;
	private Jedis resultStore;
	private Set<String> type1Hosts;
	private List<Jedis> type1Stores;
	private String channel;
	private String allChannelsKey;
	private Jedis localStore;
	private ExecutorService executorService;
	private ProgressMessageHandler progressMessageHandler;
	private CountDownLatch waitLatch;
	private CountDownLatch barrierSynch;
	private CountDownLatch progressMsgLatch;
	private boolean isWorkStealingEnabled;
	
	public Type3_1AxiomProcessor(String machineName, int localHostPort) throws Exception {
		super(machineName, localHostPort);
		this.machineName = machineName;
		propertyFileHandler = PropertyFileHandler.getInstance();
		localHostInfo = propertyFileHandler.getLocalHostInfo();
		localHostInfo.setPort(localHostPort);		
		localStore = new Jedis(localHostInfo.getHost(), 
				localHostInfo.getPort(), Constants.INFINITE_TIMEOUT);
		type1Hosts = localStore.smembers(
				AxiomDistributionType.CR_TYPE1_1.toString());
		type1Hosts.addAll(localStore.smembers(
				AxiomDistributionType.CR_TYPE1_2.toString()));
		type1Stores = new ArrayList<Jedis>();
		for(String s : type1Hosts) {
			String[] hostPort = s.split(":");
			type1Stores.add(new Jedis(hostPort[0], 
					Integer.parseInt(hostPort[1]), Constants.INFINITE_TIMEOUT));
		}
		channel = localStore.get(propertyFileHandler.getChannelKey());		
		allChannelsKey = propertyFileHandler.getAllChannelsKey();
		communicationHandler = new CommunicationHandler(
				localStore.smembers(allChannelsKey));
		resultStore = new Jedis(resultNode.getHost(), 
						resultNode.getPort(), Constants.INFINITE_TIMEOUT);
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
			System.out.println("T31: Time taken for init: " + 
					Util.getElapsedTimeSecs(initStartTime));
		
		double totalChunks;
		double chunkCount;
		double progress;
		int chunkSize = propertyFileHandler.getChunkSize();
		WorkStealer workStealer = new WorkStealer();
		PipelineManager localHostPReader = new PipelineManager(
				Collections.singletonList(localHostInfo), 
				propertyFileHandler.getPipelineQueueSize());
		
		try {
			String currentIncStr = localStore.get(Constants.CURRENT_INCREMENT);
			double currentIncrement = 0;
			if(currentIncStr != null)
				currentIncrement = Double.parseDouble(currentIncStr);
			
			String localKeys = propertyFileHandler.getLocalKeys();
			Set<String> keySet = localStore.zrangeByScore(localKeys, 
					currentIncrement, currentIncrement);
			boolean continueProcessing = false;
			boolean nextIteration = true;
			int iterationCount = 1;
			boolean firstIteration = true;
			
			long allKeysStartTime = 0;
			if(isInstrumentationEnabled)
				allKeysStartTime = System.nanoTime();
			
			Set<String> allKeys = localStore.zrange(localKeys, 0, -1);
			
			if(isInstrumentationEnabled)
				System.out.println("T31: Time taken to read all keys: " + 
					Util.getElapsedTimeSecs(allKeysStartTime));
			
			System.out.println("Total Keys: " + allKeys.size());
			Util.setScore(scoreDB, Constants.KEYS_UPDATED, 0);
			// current keys from Type1 can be from multiple nodes
			for(String type1Host : type1Hosts)
				Util.setScore(scoreDB, Constants.CURRENT_KEYS, type1Host, 0);

			while(nextIteration) {
				
				long nextIterKeysStartTime = 0;
				if(isInstrumentationEnabled)
					nextIterKeysStartTime = System.nanoTime();
				
				System.out.println("\nStarting iteration-" + iterationCount);
				if(!firstIteration) {
					// get keysUpdated from Type3_2
					Set<String> keysUpdated = new HashSet<String>(
							getKeysByScore(Constants.KEYS_UPDATED, true));
					// get currKeys from Type1
					// intersect with allKeys -- that is keySet
					keySet.clear();
					keySet = new HashSet<String>(
							getKeysByScore(Constants.CURRENT_KEYS, false));
					keySet.addAll(keysUpdated);
					keySet.retainAll(allKeys);
				}
				//calculate total chunks and get current chunk. 			
				totalChunks = Math.ceil((double)keySet.size()/chunkSize);
				localStore.set(Constants.TOTAL_CHUNKS, Double.toString(totalChunks));
				//copy keySet to chunkKeys
				Pipeline p = localStore.pipelined();
				for(String s : keySet)
					p.zadd(Constants.CHUNK_KEYS, Constants.INIT_SCORE, s);
				p.sync();
				chunkCount = 0;
				
				if(isInstrumentationEnabled) {
					System.out.println("Keys: " + keySet.size());
					System.out.println("T31: Time taken to setup " +
							"iteration: " + Util.getElapsedTimeSecs(
									nextIterKeysStartTime));
				}
				
				if(keySet.isEmpty()) {
					progress = 1.0;
					sendProgressMessage(progress, iterationCount);
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
							System.out.println("T31: Time taken to execute " +
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
							double currentOrWhole;
							if(firstIteration)
								currentOrWhole = currentIncrement;
							else
								currentOrWhole = -1.0;
							
							long oneChunkStartTime = 0;
							if(isInstrumentationEnabled)
								oneChunkStartTime = System.nanoTime();
							
							boolean status = processOneWorkChunk(
												result, localHostPReader, 
												localHostInfo, 
												currentOrWhole, null);
							continueProcessing = continueProcessing || status;
							
							if(isInstrumentationEnabled)
								System.out.println("T31: Time taken for one chunk: " + 
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
						System.out.println("T31: Time taken in waiting for " +
								"stealers: " + Util.getElapsedTimeSecs(
										stealerStartTime));
				}
				
				long workStealingStartTime = 0;
				if(isInstrumentationEnabled)
					workStealingStartTime = System.nanoTime();
				
				//wait till all nodes send in their progress messages
				progressMsgLatch.await();
				workStealer.checkAndStealWork(progressMessageHandler, 
						currentIncrement, iterationCount);
				
				if(isInstrumentationEnabled)
					System.out.println("T31: Time spent in helping busy " +
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
					System.out.println("T31: Time spent on blocking wait: " + 
							Util.getElapsedTimeSecs(blockingWaitStartTime));
				
				System.out.println("nextIteration? " + nextIteration);
				continueProcessing = false;
				firstIteration = false;
				
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
					System.out.println("T31: Time taken for iteration: " + 
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
			cleanUpForNextIncrement();				
			localHostPReader.closeAll();
			localStore.disconnect();
			for(Jedis type1Store : type1Stores)
				type1Store.disconnect();
			communicationHandler.disconnect();
			resultStore.disconnect();
			cleanUp();
		}
	}
	
	private Set<String> getKeysByScore(String key, boolean isResultStore) {
		Set<String> keys = new HashSet<String>();
		if(isResultStore) {
			double currentMinScore = Util.getScore(scoreDB, key);
			Set<Tuple> keysEScores = 
				resultStore.zrangeByScoreWithScores(
					key, currentMinScore+Constants.SCORE_INCREMENT, 
					Double.POSITIVE_INFINITY);
			for(Tuple tuple : keysEScores) {
				keys.add(tuple.getElement());
				if(tuple.getScore() > currentMinScore)
					currentMinScore = tuple.getScore();
			}
			Util.setScore(scoreDB, key, currentMinScore);
		}
		else {
			int i = 0;
			for(String type1Host : type1Hosts) {
				double currentMinScore = Util.getScore(scoreDB, key, type1Host);
				Set<Tuple> tuples = type1Stores.get(i).zrangeByScoreWithScores(
						key, currentMinScore+Constants.SCORE_INCREMENT, 
						Double.POSITIVE_INFINITY);
				for(Tuple tuple : tuples) {
					keys.add(tuple.getElement());
					if(tuple.getScore() > currentMinScore)
						currentMinScore = tuple.getScore();
				}
				Util.setScore(scoreDB, key, type1Host, currentMinScore);
				i++;
			}
		}	
		return keys;
	}
	
	private void cleanUpForNextIncrement() {
		scoreDB.flushDB();
		localStore.incr(Constants.CURRENT_INCREMENT);
		localStore.del(Constants.NUM_JOBS);
/*		
		List<String> types = new ArrayList<String>();
		for(AxiomDistributionType type : AxiomDistributionType.values())
			types.add(type.toString());
		localStore.del(types.toArray(new String[0]));
*/		
	}
}
