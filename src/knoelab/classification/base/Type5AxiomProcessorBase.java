package knoelab.classification.base;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import knoelab.classification.RolePairHandler;
import knoelab.classification.init.AxiomDistributionType;
import knoelab.classification.misc.AxiomDB;
import knoelab.classification.misc.Constants;
import knoelab.classification.misc.HostInfo;
import knoelab.classification.misc.Util;
import knoelab.classification.pipeline.PipelineManager;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;

public class Type5AxiomProcessorBase extends RolePairHandler implements AxiomProcessor {

	private Jedis chunkChannelHost;
	private StringBuilder hostPortTypeMessage;
	
	public Type5AxiomProcessorBase(String machineName, int port) {
		super(port);
		HostInfo channelHostInfo = propertyFileHandler.getChannelHost();
		chunkChannelHost = new Jedis(channelHostInfo.getHost(), 
				channelHostInfo.getPort(), Constants.INFINITE_TIMEOUT);
		hostPortTypeMessage = new StringBuilder(machineName).
			append(Constants.SEPARATOR_COLON).
			append(port).append(Constants.SEPARATOR_COLON).append(
					AxiomDistributionType.CR_TYPE5.toString()).
					append(Constants.SEPARATOR_RATE);
	}

	@Override
	public boolean processOneWorkChunk(List<String> chunkKeys,
			PipelineManager pipelinedReader, HostInfo hostInfo,
			Double currentIncrement, Jedis keyStore) throws Exception {
		boolean continueProcessing = false;
		List<Response<Set<String>>> smemberResponseList4 = 
			new ArrayList<Response<Set<String>>>();
		boolean axiomUpdate;
		keyStore.select(4);
		Pipeline db4Pipeline = keyStore.pipelined();
		
		long keysReadStartTime = 0;
		if(isInstrumentationEnabled)
			keysReadStartTime = System.nanoTime();
		
		List<Response<Set<String>>> smemberResponseList1 = getValuesWithoutScore(
				chunkKeys, pipelinedReader, hostInfo, 
				smemberResponseList4, db4Pipeline, 
				AxiomDB.ROLE_DB);
		
		if(isInstrumentationEnabled)
			System.out.println("T5: Time taken to read axiomKey values from " + 
					hostInfo.toString() + ": " + Util.getElapsedTimeSecs(
							keysReadStartTime));
		
		long applyRuleStartTime = 0;
		if(isInstrumentationEnabled)
			applyRuleStartTime = System.nanoTime();
		
		int i = 0;
		for(String key : chunkKeys) {						
			axiomUpdate = applyRule(key, 
						smemberResponseList1.get(i).get(), 
						smemberResponseList4.get(i).get());
			continueProcessing = continueProcessing || axiomUpdate;						
			i++;
		}
		
		if(isInstrumentationEnabled)
			System.out.println("T5: Time taken to applyRule() on the chunk: " + 
					Util.getElapsedTimeSecs(applyRuleStartTime));

		chunkKeys.clear();
		pipelinedReader.resetSynchResponse();
		smemberResponseList1.clear();
		smemberResponseList4.clear();
		
		long postProcessingStartTime = 0;
		if(isInstrumentationEnabled)
			postProcessingStartTime = System.nanoTime();
		
		boolean domainRangeUpdates = insertDomainRangeKV();
		boolean c5ruleUpdates = insertTypeBottomKV();
		continueProcessing = continueProcessing || domainRangeUpdates 
							|| c5ruleUpdates;
		
		if(isInstrumentationEnabled)
			System.out.println("T5: Time taken for post-processing " +
					"(insertDomainRangeKV, insertTypeBottomKV): " + 
					Util.getElapsedTimeSecs(postProcessingStartTime));
		
		return continueProcessing;
	}
	
	private List<Response<Set<String>>> getValuesWithoutScore(
			List<String> keySet, 
			PipelineManager localHostPReader, HostInfo hostInfo,
			List<Response<Set<String>>> smemberResponseList, 
			Pipeline pipeline, AxiomDB axiomDB) {
		for(String key : keySet) {
			//get the values for this key
			localHostPReader.psmembers(hostInfo, key, axiomDB);			
			// get values of this key from DB-4
			smemberResponseList.add(pipeline.smembers(key));
		}
		pipeline.sync();
		localHostPReader.synchAll(axiomDB);
		return localHostPReader.getSmembersResponseList();
	}
	
	private boolean applyRule(String axiomKey, Set<String> xvals, 
			Set<String> zvals) throws Exception {
		if(xvals.isEmpty() || zvals.isEmpty()) {
			xvals.clear();
			zvals.clear();
			return false;
		}
		Set<String> superRoleChains = null;	
		List<String> conceptRole = Util.unpackIDs(axiomKey);
		superRoleChains = roleChainLHS1Cache.get(conceptRole.get(1));
		if(superRoleChains == null)
			superRoleChains = type5ShardedJedis.smembers(conceptRole.get(1));
		if(superRoleChains == null)
			throw new Exception("r in r o s < t now found: " + 
					conceptRole.get(1));
		else
			roleChainLHS1Cache.put(conceptRole.get(1), superRoleChains);		
		numUpdates = new Long(0);
		// get 't' value in r o s < t axiom
		// then for each x, for each z; produce t = (X,Z)
		for(String superRole : superRoleChains) {
			List<String> lhsSuperRole = Util.unpackIDs(superRole);
			for(String z : zvals) {
				StringBuilder newAxiomKey = new StringBuilder(z).
					append(lhsSuperRole.get(1));
				numUpdates += insertRolePair(newAxiomKey.toString(), xvals, 
						true);
			}
		}
		xvals.clear();
		zvals.clear();
		numUpdates += kvInsertion(type32HostJedisMap, type32HostKVWrapperMap);
		numUpdates += kvInsertion(type4HostJedisMap, type4HostKVWrapperMap);
		if(type5PipelineManager1 != null)
			type5PipelineManager1.synchAll(AxiomDB.ROLE_DB);
		if(type5PipelineManager4 != null)
			type5PipelineManager4.synchAll(AxiomDB.DB4);
//		numUpdates += kvInsertion(type5HostJedisMap, type5HostKVWrapperMap);
		return (numUpdates > 0)?true:false;
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
		super.cleanUp();
		if(chunkChannelHost != null)
			chunkChannelHost.disconnect();
	}
}
