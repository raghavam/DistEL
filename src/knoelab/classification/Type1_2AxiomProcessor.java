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

import knoelab.classification.base.Type1_2AxiomProcessorBase;
import knoelab.classification.controller.CommunicationHandler;
import knoelab.classification.init.AxiomDistributionType;
import knoelab.classification.misc.AxiomDB;
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
import redis.clients.jedis.Tuple;

/**
 * Implements completion rule 2 i.e. axioms of type A1 ^ .... ^ An < B.
 * 
 * @author Raghava
 *
 */
public class Type1_2AxiomProcessor extends Type1_2AxiomProcessorBase {
	
	private Jedis scoreDB;
	private PipelineManager pipelinedReader;
	private HostInfo localHostInfo;
	private Jedis localStore;
	private String machineName;
	private CommunicationHandler communicationHandler;
	private String channel;
	private Set<String> type1Hosts;
	private List<Jedis> type1Stores;
	private String allChannelsKey;
	private ExecutorService executorService;
	private ProgressMessageHandler progressMessageHandler;
	private CountDownLatch waitLatch;
	private CountDownLatch barrierSynch;
	private CountDownLatch progressMsgLatch;
	private boolean isWorkStealingEnabled;
	
	public Type1_2AxiomProcessor(String machineName, int port) {
		super(machineName, port);
		this.machineName = machineName;
		localHostInfo = propertyFileHandler.getLocalHostInfo();
		localHostInfo.setPort(port);
		int queueSize = propertyFileHandler.getPipelineQueueSize();
		pipelinedReader = new PipelineManager(Collections.singletonList(localHostInfo), queueSize);
		scoreDB = new Jedis(localHostInfo.getHost(), port, 
							Constants.INFINITE_TIMEOUT);
		scoreDB.select(AxiomDB.SCORE_DB.getDBIndex());
		localStore = new Jedis(localHostInfo.getHost(), port, 
							Constants.INFINITE_TIMEOUT);
		localStore.connect();
		channel = localStore.get(propertyFileHandler.getChannelKey());
		type1Hosts = localStore.zrange(
							AxiomDistributionType.CR_TYPE1_1.toString(), 
							Constants.RANGE_BEGIN, Constants.RANGE_END);
		type1Hosts.addAll(localStore.zrange(
					AxiomDistributionType.CR_TYPE1_2.toString(), 
					Constants.RANGE_BEGIN, Constants.RANGE_END));
		type1Stores = new ArrayList<Jedis>();
		for(String s : type1Hosts) {
			String[] hostPort = s.split(":");
			type1Stores.add(new Jedis(hostPort[0], 
					Integer.parseInt(hostPort[1]), Constants.INFINITE_TIMEOUT));
		}		
		allChannelsKey = propertyFileHandler.getAllChannelsKey();
		communicationHandler = new CommunicationHandler(
				localStore.smembers(allChannelsKey));
		isWorkStealingEnabled = propertyFileHandler.isWorkStealingEnabled();
		if(isWorkStealingEnabled) {
			waitLatch = new CountDownLatch(1);
			barrierSynch = new CountDownLatch(1);
			progressMsgLatch = new CountDownLatch(1);
			executorService = Executors.newSingleThreadExecutor();
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
			System.out.println("T12: Time taken for init: " + 
				Util.getElapsedTimeSecs(initStartTime));
		
		PipelineManager pipelineSetReader = new PipelineManager(
				Collections.singletonList(localHostInfo),
				propertyFileHandler.getPipelineQueueSize());
		try {			
				boolean continueProcessing = false;
				boolean nextIteration = true;
				int iterationCount = 1;
				int chunkSize = propertyFileHandler.getChunkSize();
				double totalChunks;
				double chunkCount;
				double progress;
				String localKeys = propertyFileHandler.getLocalKeys();
				String currentIncStr = localStore.get(Constants.CURRENT_INCREMENT);
				double currentIncrement = 0;
				if(currentIncStr != null)
					currentIncrement = Double.parseDouble(currentIncStr);
				
				localStore.select(AxiomDB.CONJUNCT_INDEX_DB.getDBIndex());
				
				long allKeysStartTime = 0;
				if(isInstrumentationEnabled)
					allKeysStartTime = System.nanoTime();
				
				Set<String> conjunctIndices = localStore.keys("*");
				
				if(isInstrumentationEnabled)
					System.out.println("T12: Time taken to read all keys: " + 
						Util.getElapsedTimeSecs(allKeysStartTime));
				
				localStore.select(0);
				Util.setScore(scoreDB, Constants.KEYS_UPDATED, 0);
				for(String type1Host : type1Hosts)
					Util.setScore(scoreDB, Constants.CURRENT_KEYS, type1Host, 0);
				boolean isFirstIteration = true;
				Set<String> keySet;
				WorkStealer workStealer = new WorkStealer();
				
				while(nextIteration) {
					System.out.println("Starting iteration-" + iterationCount);
					
					long nextIterKeysStartTime = 0;
//					if(isInstrumentationEnabled)
						nextIterKeysStartTime = System.nanoTime();
					
					if(isFirstIteration) {
						keySet = localStore.zrangeByScore(localKeys, 
								currentIncrement, currentIncrement);
						isFirstIteration = false;
					}
					else {
						// get the keys updated by Type3_2
						Set<String> keysUpdated = new HashSet<String>(
								getKeysByScore(Constants.KEYS_UPDATED, false));
						
						// add CURRENT_KEYS of all Type1 hosts
						Set<String> totalKeysUpdated = new HashSet<String>(
								getKeysByScore(Constants.CURRENT_KEYS, true));
						// add them to the current keys to be processed
						totalKeysUpdated.addAll(keysUpdated);
						
						keySet = new HashSet<String>();
						Set<String> commonConjuncts = new HashSet<String>(conjunctIndices);
						commonConjuncts.retainAll(totalKeysUpdated);
						for(String conjunctIndex : commonConjuncts)
							pipelineSetReader.psmembers(localHostInfo, 
								conjunctIndex, AxiomDB.CONJUNCT_INDEX_DB);
						pipelineSetReader.synchAll(AxiomDB.CONJUNCT_INDEX_DB);
						List<Response<Set<String>>> conjunctMembers = 
							pipelineSetReader.getSmembersResponseList();
						for(Response<Set<String>> members : conjunctMembers)
							keySet.addAll(members.get());
						pipelineSetReader.resetSynchResponse();
						commonConjuncts.clear();
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
//					if(isInstrumentationEnabled)
						System.out.println("Keys: " + keySet.size());
					if(keySet.isEmpty()) {
						progress = 1.0;
						sendProgressMessage(progress, iterationCount);
					}
					
					if(isInstrumentationEnabled)
						System.out.println("T12: Time taken to setup " +
								"iteration: " + Util.getElapsedTimeSecs(
										nextIterKeysStartTime));
					
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
							System.out.println("T12: Time taken to execute " +
									"decrAndGetChunk script: " + 
									Util.getElapsedTimeSecs(readChunkStartTime));
						
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
							if(isFirstIteration)
								currentOrWhole = currentIncrement;
							else
								currentOrWhole = -1.0;
							
							long oneChunkStartTime = 0;
							if(isInstrumentationEnabled)
								oneChunkStartTime = System.nanoTime();
							
							boolean status = processOneWorkChunk(
									result, 
									pipelinedReader, localHostInfo, 
									currentOrWhole, localStore);
							continueProcessing = continueProcessing || status;	
							
							if(isInstrumentationEnabled)
								System.out.println("T12: Time taken for one chunk: " + 
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
							System.out.println("T12: Time taken in waiting for " +
									"stealers: " + Util.getElapsedTimeSecs(
											stealerStartTime));
						
					}				
					
					long workStealingStartTime = 0;
//					if(isInstrumentationEnabled)
						workStealingStartTime = System.nanoTime();
					
					//wait till all nodes send in their progress messages
					progressMsgLatch.await();
					workStealer.checkAndStealWork(progressMessageHandler, 
							currentIncrement, iterationCount);
					
//					if(isInstrumentationEnabled)
						System.out.println("T12: Time spent in helping busy " +
								"nodes: " + Util.getElapsedTimeSecs(
										workStealingStartTime));
				}	
					Util.broadcastMessage(communicationHandler, machineName, 
							continueProcessing, iterationCount);
					
					long blockingWaitStartTime = 0;
//					if(isInstrumentationEnabled)
						blockingWaitStartTime = System.nanoTime();
					
					nextIteration = communicationHandler.blockingWaitAndGetStatus(
										channel, iterationCount);
					
//					if(isInstrumentationEnabled)
						System.out.println("T12: Time spent on blocking wait: " + 
								Util.getElapsedTimeSecs(blockingWaitStartTime));
					
					System.out.println("nextIteration? " + nextIteration);
					continueProcessing = false;
					keySet.clear();
					
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
					System.out.println("T12: Time taken for iteration: " + 
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
			cleanUp();
			localStore.disconnect();
			scoreDB.disconnect();
			pipelinedReader.closeAll();
			pipelineSetReader.closeAll();
			communicationHandler.disconnect();
			for(Jedis type1Store : type1Stores)
				type1Store.disconnect();
//			System.out.println("\nOverall Max Conjuncts: " + maxConjuncts);
		}
	}
	
	private Set<String> getKeysByScore(String key, boolean isLocal) {
//		double currentMinScore = Util.getScore(scoreDB, key);
		Set<Tuple> keysEScores;
		Set<String> keys = new HashSet<String>();
		if(isLocal) {
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
		else {
			double currentMinScore = Util.getScore(scoreDB, key);
			keysEScores = 
				resultStore.zrangeByScoreWithScores(
					key, 
					currentMinScore+Constants.SCORE_INCREMENT, 
					Double.POSITIVE_INFINITY);
			for(Tuple tuple : keysEScores) {
				keys.add(tuple.getElement());
			if(tuple.getScore() > currentMinScore)
				currentMinScore = tuple.getScore();
			}
			Util.setScore(scoreDB, key, currentMinScore);
		}
		return keys;
	}
	
	private void cleanUpForNextIncrement() {
		localStore.del(Constants.CURRENT_KEYS, Constants.NUM_JOBS);
/*		
		List<String> types = new ArrayList<String>();
		for(AxiomDistributionType type : AxiomDistributionType.values())
			types.add(type.toString());
		localStore.del(types.toArray(new String[0]));
*/		
		scoreDB.flushDB();
		localStore.incr(Constants.CURRENT_INCREMENT);
	}
}
