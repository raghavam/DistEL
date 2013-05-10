package knoelab.classification;

import java.util.ArrayList;
import java.util.Collections;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.Set;

import knoelab.classification.init.AxiomDistributionType;
import knoelab.classification.misc.AxiomDB;
import knoelab.classification.misc.Constants;
import knoelab.classification.misc.HostInfo;
import knoelab.classification.misc.PropertyFileHandler;
import knoelab.classification.misc.Util;
import knoelab.classification.pipeline.PipelineManager;
import knoelab.classification.pipeline.PipelinedWriter;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Response;

/**
 * This class is no longer used.
 * 
 * @author Raghava
 *
 */

@Deprecated
public abstract class AxiomProcessor {

	private HostInfo resultNode;
	private List<HostInfo> targetHostInfoList;
	private PipelineManager pipelineManager;
	private AxiomDB axiomDB;
	private List<Response<Long>> saddPipelineResponse;
	private List<Response<Long>> sunionPipelineResponse = null;
	private AxiomDistributionType axiomType;
	private final int pipelineQueueSize;
	private PropertyFileHandler propertyFileHandler;
	
	private PipelineManager pipelinedReader;
	private HostInfo localHostInfo;
	
	public AxiomProcessor(AxiomDistributionType axiomType) {
		this.axiomType = axiomType;
		propertyFileHandler = PropertyFileHandler.getInstance();
		resultNode = propertyFileHandler.getResultNode();
		targetHostInfoList = new ArrayList<HostInfo>();
		if(axiomType == AxiomDistributionType.CR_TYPE1_1 || 
			axiomType == AxiomDistributionType.CR_TYPE3_2) { 
			axiomDB = AxiomDB.NON_ROLE_DB;
			targetHostInfoList.add(resultNode);
		}
		else if(axiomType == AxiomDistributionType.CR_TYPE2 ||
				axiomType == AxiomDistributionType.CR_TYPE4) {
			axiomDB = AxiomDB.ROLE_DB;
//			targetHostInfoList.addAll(Util.getTargetHostInfoList(AxiomDistributionType.CR_TYPE3_2, 
//					AxiomDistributionType.CR_TYPE4));
		}
		else if(axiomType == AxiomDistributionType.CR_TYPE3_1) {
			axiomDB = AxiomDB.NON_ROLE_DB;
			// target hosts are Type3_2
//			targetHostInfoList.addAll(Util.getTargetHostInfoList(AxiomDistributionType.CR_TYPE3_2));
		}
		pipelineQueueSize = propertyFileHandler.getPipelineQueueSize();
		pipelineManager = new PipelineManager(targetHostInfoList, pipelineQueueSize);
		
		localHostInfo = propertyFileHandler.getLocalHostInfo();
		pipelinedReader = new PipelineManager(Collections.singletonList(localHostInfo), pipelineQueueSize);
	}
	
	abstract protected void applyRule(String axiomKey, 
			Set<String> axiomValue, PipelinedWriter pipelineManager);
	
	abstract protected void closeConnection();
	
	/**
	 * This method reads in all the axioms in the localhost and applies
	 * one of the completion rules which is determined by the value of "type" key.
	 * This method applies the Template Method pattern.
	 * @throws Exception
	 */
	public void processRules() throws Exception {
		
		HostInfo localhostInfo = propertyFileHandler.getLocalHostInfo();
		Jedis localStore = new Jedis(localhostInfo.getHost(), localhostInfo.getPort(), Constants.INFINITE_TIMEOUT); 
		
		try {
			int iterationCount = 1;
			GregorianCalendar startTime = new GregorianCalendar();
			String localKeys = propertyFileHandler.getLocalKeys();
			
			boolean infiniteLoop = true;
			while(infiniteLoop) {
				GregorianCalendar iterStart = new GregorianCalendar();
				// TODO: check if read pipeline of keys would give better performance?
				double keyCount = 0;
				int multiplier = 1;
				Set<String> keySet = localStore.smembers(localKeys);
				System.out.println("Axioms to process: " + keySet.size());
				for(String axiomKey : keySet)
					pipelinedReader.psmembers(localHostInfo, axiomKey, AxiomDB.NON_ROLE_DB);
				pipelinedReader.synchAndCloseAll(AxiomDB.NON_ROLE_DB);
				pipelinedReader.getSmembersResponseList();
				
				for(String axiomKey : keySet) {
					Set<String> axiomValues = localStore.smembers(axiomKey);
					applyRule(axiomKey, axiomValues, pipelineManager);
					keyCount++;
					double keyProgress = (keyCount/keySet.size())*100;
					if(keyProgress >= (10*multiplier)) {
						System.out.println("% of no. of axioms looked at: " + keyProgress);
						multiplier++;
						checkPipelineResponse();
					}
				}
				pipelineManager.synchAll(axiomDB);
				pipelineManager.resetSynchResponse();
				
				System.out.print("\nCompleted Iteration-" + iterationCount++ + " in ");
				Util.printElapsedTime(iterStart);
				System.out.print("Total time: ");
				Util.printElapsedTime(startTime);										
			}
			saddPipelineResponse = pipelineManager.getSAddPipelineResponse();
			// only type1 and type4 use SUNIONSTORE command
			if(axiomType == AxiomDistributionType.CR_TYPE1_1 || axiomType == AxiomDistributionType.CR_TYPE4)
				sunionPipelineResponse = pipelineManager.getSUnionPipelineResponse();
			System.out.println("Iteration Done");
		}
		finally {
			localStore.disconnect();
			pipelineManager.closeAll();
			closeConnection();
		}
	}
/*	
	private void processPipelineResponse() throws Exception {
		
		if(pipelineManager.getSAddPipelineResponse().isEmpty() &&
		   pipelineManager.getSUnionPipelineResponse().isEmpty()) {
			System.out.println("sadd & sunion responses are empty");
			return;
		}		
		List<HostInfo> dependencyTargets = new ArrayList<HostInfo>();
		
		switch(axiomType) {		
		case CR_TYPE1:	// output of type1 effects Type1, Type2 and Type3_1 
						dependencyTargets.addAll(Util.getTargetHostInfoList(
								 AxiomDistributionType.CR_TYPE1, AxiomDistributionType.CR_TYPE2, 
								 AxiomDistributionType.CR_TYPE3_1));
						checkAndInsertChangedKeys(dependencyTargets);
						break;						
		case CR_TYPE2:	// output of type2 effects Type3_2 and Type4
						dependencyTargets.addAll(Util.getTargetHostInfoList(
								AxiomDistributionType.CR_TYPE3_2));
						checkAndInsertChangedKeys(dependencyTargets);
						// for Type4, the key is different from the one for Type3_2.
						// So deal with it separately
						checkAndInsertChangedKeys();
						break;			
		case CR_TYPE3_1:	// output of type3_1 effects Type3_2
						dependencyTargets.addAll(Util.getTargetHostInfoList(
								AxiomDistributionType.CR_TYPE3_2));
						checkAndInsertChangedKeys(dependencyTargets);
						break;	
		case CR_TYPE3_2:	// output of type3_2 effects Type1, Type2 and Type3_1
						dependencyTargets.addAll(Util.getTargetHostInfoList(
								AxiomDistributionType.CR_TYPE1, AxiomDistributionType.CR_TYPE2, 
								AxiomDistributionType.CR_TYPE3_1));
						checkAndInsertChangedKeys(dependencyTargets);
						break;			
		case CR_TYPE4:	// output of type4 effects Type3_2 and Type4
						dependencyTargets.addAll(Util.getTargetHostInfoList(
								AxiomDistributionType.CR_TYPE3_2));
						checkAndInsertChangedKeys(dependencyTargets);	
						// for Type4, the key is different from the one for Type3_2.
						// So deal with it separately
						checkAndInsertChangedKeys();
		}
	}
	
	private void checkAndInsertChangedKeys() throws Exception {
		List<HostInfo> targetHosts = Util.getTargetHostInfoList(AxiomDistributionType.CR_TYPE4);
		PipelineManager dependencyPipelineManager = new PipelineManager(
				targetHosts, pipelineQueueSize);
		try {
			List<Response<Long>> saddResponse = pipelineManager.getSAddPipelineResponse();
			List<String> saddKeys = pipelineManager.getPipelinedSAddKeys();
			String localKeys = propertyFileHandler.getLocalKeys();
			
			for(int i=0; i<saddResponse.size(); i++) {
				if(saddResponse.get(i).get() >= 1) {
					String conceptRole = saddKeys.get(i);
					//extract role and put it on pipeline
					dependencyPipelineManager.psadd(targetHosts.get(0), localKeys, 
						Arrays.copyOfRange(conceptRole, Constants.NUM_BYTES, conceptRole.length()), 
						AxiomDB.NON_ROLE_DB, false);
				}
			}
		}
		finally {
			dependencyPipelineManager.synchAll(AxiomDB.NON_ROLE_DB);
		}
	}
*/	
	private void checkAndInsertChangedKeys(List<HostInfo> dependencyTargets) throws Exception {
		System.out.println("\nPrinting response keys....");
//		if(axiomType != AxiomDistributionType.CR_TYPE3_1 &&
//			axiomType != AxiomDistributionType.CR_TYPE4)
//			return;
		
		PipelineManager dependencyPipelineManager = new PipelineManager(
				dependencyTargets, pipelineQueueSize);
		try {
			List<Response<Long>> saddResponse = pipelineManager.getSAddPipelineResponse();
			List<Response<Long>> sunionResponse = pipelineManager.getSUnionPipelineResponse();
			List<String> saddKeys = pipelineManager.getPipelinedSAddKeys();
			List<String> sunionKeys = pipelineManager.getPipelinedSUnionKeys();
			String localKeys = propertyFileHandler.getLocalKeys();
			
			int cnt = 0;
			for(int i=0; i<saddResponse.size(); i++) {
				if(saddResponse.get(i).get() >= 1) { // can we insert the overloaded method here?
					cnt++;
					for(HostInfo targetHost : dependencyTargets)
						dependencyPipelineManager.psadd(targetHost, localKeys, 
								saddKeys.get(i), AxiomDB.NON_ROLE_DB, false);
//					printVal(saddKeys.get(i));
				}
			}
			System.out.println("SAdd size: " + saddResponse.size() + " insertion count: " + cnt);
			long prevSize = 0;
			long currSize = 0;
			for(int i=0; i<sunionResponse.size(); i++) {
				if(i%2 == 0) 
					prevSize = sunionResponse.get(i).get();
				else {
					currSize = sunionResponse.get(i).get();
					if((currSize-prevSize) > 0) {
						// i.e. a change has been made
						for(HostInfo targetHost : dependencyTargets)
							dependencyPipelineManager.psadd(targetHost, localKeys, 
									sunionKeys.get(i/2), AxiomDB.NON_ROLE_DB, false);
//						printVal(sunionKeys.get(i/2));
					}
				}
			}
			System.out.println("SUnion size: " + sunionResponse.size() + " size/2: " + sunionResponse.size()/2);
		}
		finally {
			System.out.println("Synching dependency pipeline to....");
			for(HostInfo hinfo : dependencyTargets)
				System.out.print(hinfo.getHost()+hinfo.getPort() + "\t");
			System.out.println();
			dependencyPipelineManager.synchAndCloseAll(AxiomDB.NON_ROLE_DB);
			List<Response<Long>> responselst = dependencyPipelineManager.getSAddPipelineResponse();
			int rescnt = 0;
			for(Response<Long> response : responselst)
				if(response.get() > 0)
					rescnt++;
			System.out.println("No of insertions into the targets: " + rescnt);
		}
	}
	
	private void printVals(Set<String> vals) throws Exception {
//		HostInfo idHost = Util.getTargetHostInfoList(AxiomDistributionType.CONCEPT_ID).get(0);
//		Jedis idReader = new Jedis(idHost.getHost(), idHost.getPort());
		int count = 0;
		for(String b : vals) {
//			if(b.length == Constants.NUM_BYTES)
//				System.out.print(Util.idToConcept(b, idReader) + "  ");
//			else
//				count++;
		}
		System.out.println("Keys of len 8: " + count);
//		idReader.disconnect();
	}
	
	private void printVal(byte[] b) throws Exception {
		if(b == null) {
			System.out.println("Given key is null");
			return;
		}
		else if(b.length != Constants.NUM_BYTES) {
			System.out.println("Given key is of len: " + b.length);
			return;
		}
//		HostInfo idHost = Util.getTargetHostInfoList(AxiomDistributionType.CONCEPT_ID).get(0);
//		Jedis idReader = new Jedis(idHost.getHost(), idHost.getPort());
//		System.out.println("key: " + Util.idToConcept(b, idReader));
//		idReader.disconnect();
	}
	
	private void checkPipelineResponse() {
		List<Response<Long>> response = pipelineManager.getSAddPipelineResponse();
		System.out.println("Response size of SAdds: " + response.size());
		System.out.println("Response size of SUnionstores: " + 
				pipelineManager.getSUnionPipelineResponse().size());
		if(response.size() >= pipelineQueueSize) {
			int loopCount = response.size() / pipelineQueueSize;
			double totalInserts = 0;
			for(int i=0; i<pipelineQueueSize*loopCount; i++) 
				totalInserts += response.get(i).get();	
			System.out.println("Total inserts: " + totalInserts 
					+ " Avg. inserts: " + totalInserts/(pipelineQueueSize*loopCount) + "\n");
		}
	}
	
	public boolean isResultUpdated() {
		boolean isUpdated = false;
		
		System.out.println("Size of sadd response: " + saddPipelineResponse.size());
		if(saddPipelineResponse.size() > 0)
			System.out.println("First element of sadd response: " + saddPipelineResponse.get(0).get());
		for(Response<Long> response : saddPipelineResponse)
			if(response.get() == 1) {
				// i.e. there is an addition of either a new value or new key
				isUpdated = true;
				break;
			}
		if(!isUpdated) {
			if(axiomType == AxiomDistributionType.CR_TYPE2 || axiomType == AxiomDistributionType.CR_TYPE3_1)
				return isUpdated;
			// check SUNIONSTORE command responses
			// sunionPipelineResponse contains old size of key X and
			// new size of key X in alternate positions.
			System.out.println("Size of sunion response: " + sunionPipelineResponse.size());
			if(sunionPipelineResponse.size() > 0)
				System.out.println("First & second elements of sunion: " + 
						sunionPipelineResponse.get(0).get() + "\t" + sunionPipelineResponse.get(1).get());
			Long keyCardinality = new Long(0);
			for(int i=0; i<sunionPipelineResponse.size(); i++) {
				Response<Long> response = sunionPipelineResponse.get(i);
				if(i%2 == 0)
					keyCardinality = response.get();
				else {
					long keySizeDiff = response.get() - keyCardinality;
					if(keySizeDiff != 0) {
						isUpdated = true;
						break;
					}
				}
			}
		}
		return isUpdated;
	}
	
}

