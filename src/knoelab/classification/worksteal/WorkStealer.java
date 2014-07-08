package knoelab.classification.worksteal;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import knoelab.classification.base.AxiomProcessor;
import knoelab.classification.base.Type1_1AxiomProcessorBase;
import knoelab.classification.base.Type1_2AxiomProcessorBase;
import knoelab.classification.base.Type2AxiomProcessorBase;
import knoelab.classification.base.Type3_1AxiomProcessorBase;
import knoelab.classification.base.Type3_2AxiomProcessorBase;
import knoelab.classification.base.Type4AxiomProcessorBase;
import knoelab.classification.base.Type5AxiomProcessorBase;
import knoelab.classification.base.TypeBottomAxiomProcessorBase;
import knoelab.classification.init.AxiomDistributionType;
import knoelab.classification.misc.Constants;
import knoelab.classification.misc.HostInfo;
import knoelab.classification.misc.PropertyFileHandler;
import knoelab.classification.misc.ScriptsCollection;
import knoelab.classification.misc.Util;
import knoelab.classification.pipeline.PipelineManager;
import redis.clients.jedis.Jedis;

/**
 * This class checks whether any other Type process needs help and steals work
 * from it.
 * 
 * @author Raghava
 */
public class WorkStealer {
	
	private double progress;
	private PropertyFileHandler propertyFileHandler;
	private int chunkSize;
	
	private boolean isInstrumentationEnabled;
	
	public WorkStealer() {
		propertyFileHandler = PropertyFileHandler.getInstance();
		isInstrumentationEnabled = 
				propertyFileHandler.isInstrumentationEnabled();
		chunkSize = propertyFileHandler.getChunkSize();
	}
	
	//chunkSize could be made dynamic -- so made it an arg
	public void checkAndStealWork(ProgressMessageHandler progressMessageHandler, 
			Double currentIncrement, int iterationCount) throws Exception {
		boolean continueStealing = true;
		while(continueStealing) {
			String machineType = 
				progressMessageHandler.getLeastCompletedType(iterationCount);
			if(machineType != null) {				
				String[] machinePortType = machineType.split(
						Constants.SEPARATOR_COLON);
				HostInfo remoteHostInfo = new HostInfo(machinePortType[0], 
						Integer.parseInt(machinePortType[1]));
				Jedis remoteStore = new Jedis(remoteHostInfo.getHost(), 
						remoteHostInfo.getPort(), Constants.INFINITE_TIMEOUT);
				remoteStore.connect();
				AxiomDistributionType typeToSteal = 
					AxiomDistributionType.valueOf(machinePortType[2]);
				PipelineManager pipelinedReader = new PipelineManager(
						Collections.singletonList(remoteHostInfo), 
						propertyFileHandler.getPipelineQueueSize());
				boolean status = false;
				AxiomProcessor axiomProcessor = null;
				List<String> chunkKeys = getAWorkChunk(
						remoteStore, Constants.CHUNK_KEYS);
				System.out.println("Stealing from " + typeToSteal + 
						"  chunk: " + chunkKeys.size());
				if(chunkKeys.isEmpty())
					return;
				try {
					switch(typeToSteal) {
					case CR_TYPE1_1: 
						axiomProcessor = 
							new Type1_1AxiomProcessorBase(remoteHostInfo.getHost(), 
									remoteHostInfo.getPort());
						axiomProcessor.sendProgressMessage(progress, iterationCount);
						status = axiomProcessor.processOneWorkChunk(
								chunkKeys, pipelinedReader, remoteHostInfo, 
								-1.0, remoteStore);
						postChunkProcessing(status, remoteStore);
						break;
					case CR_TYPE1_2:
						axiomProcessor = 
							new Type1_2AxiomProcessorBase(remoteHostInfo.getHost(), 
									remoteHostInfo.getPort());
						axiomProcessor.sendProgressMessage(progress, iterationCount);
						status = axiomProcessor.processOneWorkChunk(
								chunkKeys, pipelinedReader, remoteHostInfo, 
								-1.0, remoteStore);
						postChunkProcessing(status, remoteStore);
						break;
					case CR_TYPE2:
						axiomProcessor = 
							new Type2AxiomProcessorBase(remoteHostInfo.getHost(), 
									remoteHostInfo.getPort());
						axiomProcessor.sendProgressMessage(progress, iterationCount);
						status = axiomProcessor.processOneWorkChunk(
								chunkKeys, pipelinedReader, remoteHostInfo, 
								-1.0, null);
						postChunkProcessing(status, remoteStore);
						break;
					case CR_TYPE3_1:
						axiomProcessor = 
							new Type3_1AxiomProcessorBase(remoteHostInfo.getHost(), 
									remoteHostInfo.getPort());
						axiomProcessor.sendProgressMessage(progress, iterationCount);
						status = axiomProcessor.processOneWorkChunk(
								chunkKeys, pipelinedReader, remoteHostInfo, 
								-1.0, null);
						postChunkProcessing(status, remoteStore);
						break;
					case CR_TYPE3_2:
						axiomProcessor = 
							new Type3_2AxiomProcessorBase(remoteHostInfo.getHost(), 
									remoteHostInfo.getPort());
						axiomProcessor.sendProgressMessage(progress, iterationCount);
						//part-1
						boolean status1 = axiomProcessor.processOneWorkChunk(
								chunkKeys, pipelinedReader, remoteHostInfo, 
								1.0, null);
						postChunkProcessing(status1, remoteStore);
						//part-2
						chunkKeys = getAWorkChunk(
								remoteStore, Constants.CHUNK_KEYS2);
						if(!chunkKeys.isEmpty()) {
							axiomProcessor.sendProgressMessage(progress, 
									iterationCount);
							boolean status2 = axiomProcessor.processOneWorkChunk(
								chunkKeys, pipelinedReader, remoteHostInfo, 
								-1.0, null);
							status = status2; //post processing is handled below
							postChunkProcessing(status, remoteStore);
						}
						break;
					case CR_TYPE4:
						axiomProcessor = 
							new Type4AxiomProcessorBase(remoteHostInfo.getHost(), 
									remoteHostInfo.getPort());
						axiomProcessor.sendProgressMessage(progress, iterationCount);
						status = axiomProcessor.processOneWorkChunk(
								chunkKeys, pipelinedReader, remoteHostInfo, 
								null, null);
						postChunkProcessing(status, remoteStore);
						break;
					case CR_TYPE5:
						axiomProcessor = 
							new Type5AxiomProcessorBase(remoteHostInfo.getHost(), 
									remoteHostInfo.getPort());
						axiomProcessor.sendProgressMessage(progress, iterationCount);
						status = axiomProcessor.processOneWorkChunk(
								chunkKeys, pipelinedReader, remoteHostInfo, 
								null, remoteStore);
						postChunkProcessing(status, remoteStore);
						break;
					case CR_TYPE_BOTTOM:
						axiomProcessor = 
							new TypeBottomAxiomProcessorBase(remoteHostInfo.getHost(), 
									remoteHostInfo.getPort());
						axiomProcessor.sendProgressMessage(progress, iterationCount);
						status = axiomProcessor.processOneWorkChunk(
								chunkKeys, null, null, null, null);
						postChunkProcessing(status, remoteStore);
						break;
					default: 
						throw new Exception("Unknown axiom type: " + typeToSteal);	
					}
				} finally {
					axiomProcessor.cleanUp();
					remoteStore.disconnect();
					pipelinedReader.closeAll();
				}				
			}
			else
				continueStealing = false;
		}
	}
	
	private void postChunkProcessing(boolean status, Jedis remoteStore) 
			throws Exception {
		String intStatus = status ? "1" : "0";	
		remoteStore.select(0);
		Long chunkCount = (Long) remoteStore.eval(
				ScriptsCollection.processedChunkStatusScript, 
				new ArrayList<String>(),
				Collections.singletonList(intStatus));
		if(chunkCount < 0)
			throw new Exception("Stealers count less than 0: " + chunkCount);
	}
	
	private List<String> getAWorkChunk(Jedis remoteStore, String chunkKey) { 
		long readChunkStartTime = 0;
		if(isInstrumentationEnabled)
			readChunkStartTime = System.nanoTime();
		
		ArrayList<String> result = 
				(ArrayList<String>) remoteStore.eval(
						ScriptsCollection.decrAndGetStealerChunk, 
				Collections.singletonList(chunkKey), 
				Collections.singletonList(String.valueOf(chunkSize)));
		
		if(isInstrumentationEnabled)
			System.out.println("WorkStealer: Time taken to execute " +
					"decrAndGetChunk script: " + 
					Util.getElapsedTimeSecs(readChunkStartTime));
		
		int chunkCount = Integer.parseInt(result.remove(0));
		double totalChunks = Double.parseDouble(result.remove(0));
		if(chunkCount == -1) {
			//all chunks are processed
			progress = 1.0;
		}
		else 
			progress = chunkCount/totalChunks;
		return result;
	}
}
