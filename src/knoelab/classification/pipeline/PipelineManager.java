package knoelab.classification.pipeline;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.Map.Entry;
import java.util.concurrent.LinkedBlockingQueue;

import knoelab.classification.misc.AxiomDB;
import knoelab.classification.misc.Constants;
import knoelab.classification.misc.HostInfo;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;
import redis.clients.jedis.Tuple;

//TODO: remove response, collectPipelineResponse and sadd/sunionKeys
public class PipelineManager implements PipelinedWriter, PipelinedReader {

	private HashMap<String, LinkedBlockingQueue<PipelineMessage>> shardQueue;
	private HashMap<String, Jedis> jedisShards;
	
	// Only one type of pipeline will be in operation - either 
	// concept or role related pipeline. So a list of lists is not required
	private List<Response<Long>> saddResponseList;
	private List<Response<Long>> sunionResponseList;
	private List<Response<Set<Tuple>>> zrangeByScoreWithScoresResponseList;
	private List<Response<Set<String>>> zrangeByScoreResponseList;
	private List<Response<Set<String>>> zrangeResponseList;
	private List<Response<Set<String>>> smembersResponseList;
	private List<Response<String>> getResponseList;
	private List<Response<String>> evalResponseList;
	private List<Response<Double>> zscoreResponseList;
	
	private boolean synchDone;
	private List<String> saddKeys;
	private List<String> sunionKeys;
	
	public PipelineManager(List<HostInfo> hostInfoList, int queueBlockingSize) {
		shardQueue = new HashMap<String, LinkedBlockingQueue<PipelineMessage>>(
				hostInfoList.size());
		jedisShards = new HashMap<String, Jedis>(hostInfoList.size());
		saddResponseList = new ArrayList<Response<Long>>();
		sunionResponseList = new ArrayList<Response<Long>>();
		zrangeByScoreWithScoresResponseList = new ArrayList<Response<Set<Tuple>>>();
		zrangeByScoreResponseList = new ArrayList<Response<Set<String>>>();
		zrangeResponseList = new ArrayList<Response<Set<String>>>();
		zscoreResponseList = new ArrayList<Response<Double>>();
		smembersResponseList = new ArrayList<Response<Set<String>>>();
		getResponseList = new ArrayList<Response<String>>();
		evalResponseList = new ArrayList<Response<String>>();
		synchDone = false;
		saddKeys = new ArrayList<String>();
		sunionKeys = new ArrayList<String>();
		
		for(HostInfo hostInfo : hostInfoList) {
			String mapKey = hostInfo.getHost() + hostInfo.getPort();
			LinkedBlockingQueue<PipelineMessage> pipelineQueue = 
				new LinkedBlockingQueue<PipelineMessage>(queueBlockingSize);
			Jedis jedis = new Jedis(hostInfo.getHost(), hostInfo.getPort(), 
					Constants.INFINITE_TIMEOUT);
			shardQueue.put(mapKey, pipelineQueue);
			jedisShards.put(mapKey, jedis);
		}
	}
	
	/* (non-Javadoc)
	 * @see knoelab.classification.pipeline.PipelinedWriter#pset(
	 * knoelab.classification.misc.HostInfo, byte[], byte[], 
	 * knoelab.classification.misc.AxiomDB)
	 */
	@Override
	public void pset(HostInfo hostInfo, String key, String value, AxiomDB axiomDB) {
		PipelineMessage pmessage = new PipelineMessage(key, value, PipelineMessageType.SET, false);
		insert(hostInfo, pmessage, axiomDB);
	}
	
	@Override
	public void pget(HostInfo hostInfo, String key, AxiomDB axiomDB) {
		PipelineMessage pmessage = new PipelineMessage(key, null, 
				PipelineMessageType.GET, true);
		insert(hostInfo, pmessage, axiomDB);
	}
	
	/* (non-Javadoc)
	 * @see knoelab.classification.pipeline.PipelinedWriter#psadd(
	 * knoelab.classification.misc.HostInfo, byte[], byte[], 
	 * knoelab.classification.misc.AxiomDB, boolean)
	 */
	@Override
	public void psadd(HostInfo hostInfo, String key, String value,
			AxiomDB axiomDB, boolean collectPipelineResponse) throws Exception {
		if(key == null || value == null)
			throw new Exception("Key or Value is null. Key: " + 
					key + "  Value: " + value);
		PipelineMessage pmessage = new PipelineMessage(key, value, 
					PipelineMessageType.SADD, collectPipelineResponse);
		insert(hostInfo, pmessage, axiomDB);
	}
	
	/* (non-Javadoc)
	 * @see knoelab.classification.pipeline.PipelinedWriter#psunionstore(
	 * knoelab.classification.misc.HostInfo, byte[], byte[], 
	 * knoelab.classification.misc.AxiomDB, boolean)
	 */
	@Override
	public void psunionstore(HostInfo hostInfo, String key, String value, 
			AxiomDB axiomDB, boolean collectPipelineResponse) {
		PipelineMessage pmessage = new PipelineMessage(key, value, 
				PipelineMessageType.SUNIONSTORE, collectPipelineResponse);
		insert(hostInfo, pmessage, axiomDB);		
	}
	
	/* (non-Javadoc)
	 * @see knoelab.classification.pipeline.PipelinedWriter#pzadd(HostInfo, byte[], double, byte[])
	 */
	@Override
	public void pzadd(HostInfo hostInfo, String key, double score, 
			String value, AxiomDB axiomDB) {
		PipelineMessageWithScore pMsgScore = new PipelineMessageWithScore(key, value, 
						score, PipelineMessageType.ZADD, false);
		insert(hostInfo, pMsgScore, axiomDB);
	}
	
	@Override
	public void pZRange(HostInfo hostInfo, String key, 
			long startIndex, long endIndex, AxiomDB axiomDB) {
		PipelineMessageWithScore pMsgScore = new PipelineMessageWithScore(key, startIndex, 
				endIndex, PipelineMessageType.ZRANGE, true);
		insert(hostInfo, pMsgScore, axiomDB);
	}
	
	@Override
	public void pZRangeByScore(HostInfo hostInfo, String key, double minScore, 
			double maxScore, AxiomDB axiomDB) {
		PipelineMessageWithScore pMsgScore = new PipelineMessageWithScore(key, minScore, 
				maxScore, PipelineMessageType.ZRANGE_BY_SCORE, true);		
		insert(hostInfo, pMsgScore, axiomDB);
	}
	
	@Override
	public void pZRangeByScoreWithScores(HostInfo hostInfo, String key, double minScore, 
			double maxScore, AxiomDB axiomDB) {
		PipelineMessageWithScore pMsgScore = new PipelineMessageWithScore(key, minScore, 
				maxScore, PipelineMessageType.ZRANGE_BY_SCORE_WITH_SCORES, true);		
		insert(hostInfo, pMsgScore, axiomDB);
	}
	
	@Override
	public boolean pZScore(HostInfo hostInfo, String key, String member, 
			AxiomDB axiomDB) {
		PipelineMessage pmessage = new PipelineMessage(key, member, 
				PipelineMessageType.ZSCORE, true);
		return insert(hostInfo, pmessage, axiomDB);
	}

	@Override
	public void psmembers(HostInfo hostInfo, String key, AxiomDB axiomDB) {
		PipelineMessage pmessage = new PipelineMessage(key, null, 
				PipelineMessageType.SMEMBERS, true);
		insert(hostInfo, pmessage, axiomDB);
	}
	
	@Override
	public void phset(HostInfo hostInfo, String key, String field, 
			String value, AxiomDB axiomDB) {
		PipelineMessageWithField pmessage = new PipelineMessageWithField(key, 
				field, value, PipelineMessageType.HSET, false);
		insert(hostInfo, pmessage, axiomDB);
	}
	
	public void peval(HostInfo hostInfo, String script, List<String> keys, 
			List<String> args, AxiomDB axiomDB) {
		PipelineMessageWithScript pmessage = new PipelineMessageWithScript(
				script, keys, args);
		insert(hostInfo, pmessage, axiomDB);
	}
	
	private boolean insert(HostInfo hostInfo, PipelineMessage pipelineMessage, 
			AxiomDB axiomDB) {
		String mapKey = hostInfo.getHost() + hostInfo.getPort();			
		LinkedBlockingQueue<PipelineMessage> queue = shardQueue.get(mapKey);
		boolean insertSuccessful = queue.offer(pipelineMessage);		
		if(!insertSuccessful) {
			synchQueue(mapKey, queue, axiomDB);	
			synchDone = true;
			// queue would be empty now. Insert (k,v)
			queue.offer(pipelineMessage);
		}
		return insertSuccessful;
	}
	
	private void synchQueue(String mapKey, LinkedBlockingQueue<PipelineMessage> queue, 
			AxiomDB axiomDB) {
		
		if(queue.isEmpty())
			return; 

		Jedis jedis = jedisShards.get(mapKey);
		jedis.select(axiomDB.getDBIndex());
		try {
		List<PipelineMessage> msglist = new ArrayList<PipelineMessage>(queue.size());
		// javadoc says draining is faster than repeated polling
		queue.drainTo(msglist);
		Pipeline p = jedis.pipelined();

		for(PipelineMessage pipelineMsg : msglist) {

			switch(pipelineMsg.getMessageType()) {
				case SET: 	p.set(pipelineMsg.getKey(), pipelineMsg.getValue());
							break;
				
				case SADD: 	Response<Long> saddResponse = p.sadd(
								pipelineMsg.getKey(), pipelineMsg.getValue());
							if(pipelineMsg.isCollectPipelineResponse()) {
								saddResponseList.add(saddResponse);
								saddKeys.add(pipelineMsg.getKey());
							}
							break;
				
				case SUNIONSTORE: 	if(pipelineMsg.isCollectPipelineResponse()) {
										sunionResponseList.add(p.scard(pipelineMsg.getKey()));
										sunionResponseList.add(p.sunionstore(pipelineMsg.getKey(), 
												pipelineMsg.getKey(), pipelineMsg.getValue()));
										sunionKeys.add(pipelineMsg.getKey());
									}
									else
										p.sunionstore(pipelineMsg.getKey(), 
												pipelineMsg.getKey(), pipelineMsg.getValue());
									break;
									
				case ZADD:	p.zadd(pipelineMsg.getKey(), 
								((PipelineMessageWithScore)pipelineMsg).getScore(), 
								((PipelineMessageWithScore)pipelineMsg).getValue());
							break;
							
				case ZRANGE_BY_SCORE: 	zrangeByScoreResponseList.add(
								p.zrangeByScore(
								((PipelineMessageWithScore)pipelineMsg).getKey(), 
								((PipelineMessageWithScore)pipelineMsg).getMinScore(), 
								((PipelineMessageWithScore)pipelineMsg).getMaxScore()));
								break;		
									
				case ZRANGE_BY_SCORE_WITH_SCORES:	zrangeByScoreWithScoresResponseList.add(
										p.zrangeByScoreWithScores(
										((PipelineMessageWithScore)pipelineMsg).getKey(), 
										((PipelineMessageWithScore)pipelineMsg).getMinScore(), 
										((PipelineMessageWithScore)pipelineMsg).getMaxScore()));
										break;
				case ZRANGE: zrangeResponseList.add(p.zrange(
							((PipelineMessageWithScore)pipelineMsg).getKey(), 
								(int)((PipelineMessageWithScore)pipelineMsg).getMinScore(), 
								(int)((PipelineMessageWithScore)pipelineMsg).getMaxScore()));
							 break;
							 
				case ZSCORE: zscoreResponseList.add(p.zscore(
								pipelineMsg.getKey(), pipelineMsg.getValue()));
							 break;	
							
				case SMEMBERS:	smembersResponseList.add(p.smembers(pipelineMsg.getKey()));
								break;
								
				case GET: 	getResponseList.add(p.get(pipelineMsg.getKey()));	
							break;
							
				case HSET:  p.hset(
							pipelineMsg.getKey(), 
							((PipelineMessageWithField)pipelineMsg).getField(), 
							pipelineMsg.getValue());		
							break;
							
				case EVAL:	PipelineMessageWithScript pmessage = 
									(PipelineMessageWithScript) pipelineMsg;
							evalResponseList.add(p.eval(pmessage.getScript(), 
									pmessage.getScriptKeys(), 
									pmessage.getScriptArgs()));
							break;			
				
				default: try { throw new Exception("Unexpected Pipeline Message Type: " 
									+ pipelineMsg.getMessageType()); } 
						 catch (Exception e) { e.printStackTrace(); }
			}
		}
		p.sync();
		msglist.clear();
		}
		catch(Exception e) { 
			System.out.println("Problem with jedis on " + mapKey + 
					"  Jedis isConnected: " + jedis.isConnected()); 
			closeAll();
			e.printStackTrace(); 
		}
	}
	
	public List<Response<Long>> getSAddPipelineResponse() {
		return saddResponseList;
	}
	
	public List<Response<Long>> getSUnionPipelineResponse() {
		return sunionResponseList;
	}
	
	/**
	 * Selectively synch a particular set of queues. This method is 
	 * useful when reads need to work on the present consistent 
	 * state of a particular shard.
	 * 
	 * @param shardKey 
	 */
	public void selectiveSynch(AxiomDB axiomDB, HostInfo... hostsInfo) {
		for(HostInfo hinfo : hostsInfo) {
			String mapKey = hinfo.getHost() + hinfo.getPort();
			synchQueue(mapKey, shardQueue.get(mapKey), axiomDB);	
		}
	}
	
	/**
	 * Flush all the queues and process the messages (pipeline sync all)
	 */
	public void synchAll(AxiomDB axiomDB) {
		Set<Entry<String, LinkedBlockingQueue<PipelineMessage>>> 
						shardQueueEntries = shardQueue.entrySet();
		for(Entry<String, LinkedBlockingQueue<PipelineMessage>> 
									entry : shardQueueEntries)
			synchQueue(entry.getKey(), entry.getValue(), axiomDB);
		synchDone = true;
	}

	/**
	 * Close all jedis connections
	 */
	public void closeAll() {
		Collection<Jedis> jedisList = jedisShards.values();
		for(Jedis jedis : jedisList)
			jedis.disconnect();
	}
	
	public void synchAndCloseAll(AxiomDB axiomDB) {
		synchAll(axiomDB);
		closeAll();
	}
	
	public boolean isSynchDone() {
		return synchDone;
	}
	
	public void resetSynchResponse() {
		// Note: pipeline is synched either when queue reaches the limit or 
		// when key count is 0.
		
		synchDone = false;
		saddResponseList.clear();
		saddKeys.clear();
		sunionResponseList.clear();
		sunionKeys.clear();
		smembersResponseList.clear();
		zrangeByScoreResponseList.clear();
		zrangeByScoreWithScoresResponseList.clear();
		zrangeResponseList.clear();
		getResponseList.clear();
		zscoreResponseList.clear();
		evalResponseList.clear();
	}
	
	public List<String> getPipelinedSAddKeys() {
		return saddKeys;
	}
	
	public List<String> getPipelinedSUnionKeys() {
		return sunionKeys;
	}
	
	public List<Response<Set<String>>> getSmembersResponseList() {
		return smembersResponseList;
	}
	
	public List<Response<Set<String>>> getZrangeByScoreResponseList() {
		return zrangeByScoreResponseList;
	}
	
	public List<Response<Set<Tuple>>> getZrangeByScoreWithScoresResponseList() {
		return zrangeByScoreWithScoresResponseList;
	}
	
	public List<Response<Set<String>>> getZRangeResponseList() {
		return zrangeResponseList;
	}
	
	public List<Response<String>> getResponseList() {
		return getResponseList;
	}
	
	public List<Response<Double>> getZScoreResponseList() {
		return zscoreResponseList;
	}
	
	public List<Response<String>> getEvalResponseList() {
		return evalResponseList;
	}
}

