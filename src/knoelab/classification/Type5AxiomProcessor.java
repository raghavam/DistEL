package knoelab.classification;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.GregorianCalendar;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import knoelab.classification.controller.CommunicationHandler;
import knoelab.classification.misc.AxiomDB;
import knoelab.classification.misc.Constants;
import knoelab.classification.misc.Util;
import knoelab.classification.pipeline.PipelineManager;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;
import redis.clients.jedis.Tuple;

/**
 * Implements role chain axioms, r o s < t i.e.,
 * completion rule R5.
 * 
 * @author Raghava
 *
 */
public class Type5AxiomProcessor extends RolePairHandler {

	private CommunicationHandler communicationHandler;
	private String machineName;
	private String channel;
	private Jedis scoreDB1;
	private Jedis scoreDB2;
	private final int DB5 = 5;
	private final int DB6 = 6;
	
	public Type5AxiomProcessor(int localHostPort) {
		super(localHostPort);
		scoreDB1 = new Jedis(localHostInfo.getHost(), 
				localHostInfo.getPort(), Constants.INFINITE_TIMEOUT);
		scoreDB1.select(DB5);
		scoreDB2 = new Jedis(localHostInfo.getHost(), 
				localHostInfo.getPort(), Constants.INFINITE_TIMEOUT);
		scoreDB2.select(DB6);
		channel = localStore.get(propertyFileHandler.getChannelKey());		
		String allChannelsKey = propertyFileHandler.getAllChannelsKey();
		communicationHandler = new CommunicationHandler(
				localStore.smembers(allChannelsKey));
		try {
			machineName = InetAddress.getLocalHost().getHostName();
		}
		catch(Exception e) { e.printStackTrace(); }
	}
	
	public void processRules() throws Exception {
		PipelineManager localHostPReader = new PipelineManager(
				Collections.singletonList(localHostInfo), 
				propertyFileHandler.getPipelineQueueSize());
//		List<Response<Set<Tuple>>> zrangeResponseList;
		List<Response<Set<String>>> smemberResponseList1; 
		List<Response<Set<String>>> smemberResponseList4 = 
				new ArrayList<Response<Set<String>>>();
		double score1 = Constants.INIT_SCORE;
		int iterationCount = 1;
		try {
			boolean continueProcessing = false;
			boolean continue1 = false;
			boolean continue2 = false;
			boolean nextIteration = true;
			boolean axiomUpdate;
			String localKeys = propertyFileHandler.getLocalKeys();
			
			while(nextIteration) {
				// Phase-1; match Yr from DB-1 to Xr from DB-4 
				localStore.select(1);
				Set<String> keySet1 = localStore.smembers(localKeys);
				System.out.println("\nAxioms to process in phase-1: " + 
						keySet1.size());
				if(keySet1.isEmpty()) 
					continue1 = false;				
				else {
					GregorianCalendar start = new GregorianCalendar();
					
					localStore.select(4);
					Pipeline db4Pipeline = localStore.pipelined();
					smemberResponseList1 = getValuesWithoutScore(
							keySet1, localHostPReader, 
							smemberResponseList4, db4Pipeline, 
							AxiomDB.ROLE_DB);
					
					int i = 0;
					for(String key : keySet1) {						
						axiomUpdate = applyRule(key, 
									smemberResponseList1.get(i).get(), 
									smemberResponseList4.get(i).get());
						continue1 = continue1 || axiomUpdate;						
						i++;
					}
					keySet1.clear();
					score1 += Constants.SCORE_INCREMENT;
					localHostPReader.resetSynchResponse();
					smemberResponseList1.clear();
					smemberResponseList4.clear();
					
					System.out.println("Time taken for this iteration (phase1)");
					Util.printElapsedTime(start);
				}		
				continueProcessing = continue1 || continue2;				
				nextIteration = continueWithNextIteration(
						continueProcessing, iterationCount);
				iterationCount++;
				System.out.println("nextIteration? " + nextIteration);
				continue1 = false;
				continue2 = false;
			}
		}
		finally {
			localHostPReader.closeAll();
			communicationHandler.disconnect();
			scoreDB1.disconnect();
			scoreDB2.disconnect();
			cleanUp();
		}
	}
	
	private List<Response<Set<String>>> getValuesWithoutScore(
			Set<String> keySet, 
			PipelineManager localHostPReader, 
			List<Response<Set<String>>> smemberResponseList, 
			Pipeline pipeline, AxiomDB axiomDB) {
		for(String key : keySet) {
			//get the values for this key
			localHostPReader.psmembers(localHostInfo, key, axiomDB);			
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
		String[] conceptRole = 
			axiomKey.split(propertyFileHandler.getComplexAxiomSeparator());
		superRoleChains = roleChainLHS1Cache.get(conceptRole[1]);
		if(superRoleChains == null)
			superRoleChains = type5ShardedJedis.smembers(conceptRole[1]);
		if(superRoleChains == null)
			throw new Exception("r in r o s < t now found: " + 
					conceptRole[1]);
		else
			roleChainLHS1Cache.put(conceptRole[1], superRoleChains);		
		numUpdates = new Long(0);
		// get 't' value in r o s < t axiom
		// then for each x, for each z; produce t = (X,Z)
		for(String superRole : superRoleChains) {
			String[] lhsSuperRole = superRole.split(
					propertyFileHandler.getComplexAxiomSeparator());
			for(String z : zvals) {
				StringBuilder newAxiomKey = new StringBuilder(z).
					append(propertyFileHandler.getComplexAxiomSeparator()).
					append(lhsSuperRole[1]);
				numUpdates += insertRolePair(newAxiomKey.toString(), xvals);
			}
		}
		xvals.clear();
		zvals.clear();
		numUpdates += kvInsertion(type32HostJedisMap, type32HostKVWrapperMap);
		numUpdates += kvInsertion(type4HostJedisMap, type4HostKVWrapperMap);
		type5PipelineManager1.synchAll(AxiomDB.ROLE_DB);
		type5PipelineManager4.synchAll(AxiomDB.DB4);
//		numUpdates += kvInsertion(type5HostJedisMap, type5HostKVWrapperMap);
		return (numUpdates > 0)?true:false;
	}
	
	private boolean continueWithNextIteration(boolean currIterStatus, 
			int iterationCount) {
		String message = machineName + "~" + iterationCount;
		// 0 - update; 1 - no update
		if(currIterStatus) {
			// there are some updates from at least one axiom
			message = message + "~" + 0;
		}
		else {
			// there are no updates from any of the axioms
			message = message + "~" + 1;
		}
		System.out.println("continueProcessing? " + currIterStatus);
		communicationHandler.broadcast(message);
		System.out.println("Iteration " + iterationCount + " completed");
		return communicationHandler.removeAndGetStatus(
									channel, iterationCount);
	}
}
