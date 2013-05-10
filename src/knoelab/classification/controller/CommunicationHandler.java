package knoelab.classification.controller;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import knoelab.classification.misc.Constants;
import knoelab.classification.misc.HostInfo;
import knoelab.classification.misc.PropertyFileHandler;
import redis.clients.jedis.Jedis;

public class CommunicationHandler {

	private PropertyFileHandler propertyFileHandler;
	private Jedis channelHost;
	private int numChannels;
	private Map<String, List<String>> iterCountMsgsMap;
	private Set<String> channels;
	
	public CommunicationHandler(Set<String> channels) {
		propertyFileHandler = PropertyFileHandler.getInstance();
		HostInfo hostInfo = propertyFileHandler.getChannelHost();
		channelHost = new Jedis(hostInfo.getHost(), 
				hostInfo.getPort(), Constants.INFINITE_TIMEOUT);
		PropertyFileHandler propertyFileHandler = PropertyFileHandler.getInstance();
		// subtract conceptID & resultNode
		numChannels = propertyFileHandler.getShardCount() - 2;
		iterCountMsgsMap = new HashMap<String, List<String>>();
		this.channels = channels;
		try {
			if(channels.size() != numChannels)
				throw new Exception("No of channels: " + channels.size() + 
						";  should be " + numChannels);
		}
		catch(Exception e) {
			e.printStackTrace();
		}
	}
	
	public void broadcast(String message) {
		for(String channel : channels) 
			channelHost.lpush(channel, message);
		System.out.println("Sent msgs...");
	}
	
	public boolean removeAndGetStatus(String channel, int iterationCount) {
		// get numChannel number of messages
		String message;
		String iterCountStr = Integer.toString(iterationCount);
		int msgCount = 0;
		if(iterCountMsgsMap.containsKey(iterCountStr))
			msgCount = iterCountMsgsMap.get(iterCountStr).size();
		// numChannels or numProcesses -- both are same
		while(msgCount < numChannels) {
			List<String> response = channelHost.blpop(0, channel);
			if(response.size() == 2) {
				//was initially blocked and later unblocked 
				message = response.get(1);
			}
			else 
				message = response.get(0);
			String[] fragments = message.split("~");
			if(iterCountMsgsMap.containsKey(fragments[1])) {
				iterCountMsgsMap.get(fragments[1]).add(fragments[2]);
			}
			else {
				List<String> msgLst = new ArrayList<String>();
				msgLst.add(fragments[2]);
				iterCountMsgsMap.put(fragments[1], msgLst);
			}
			if(fragments[1].equals(iterCountStr))
				msgCount++;
		}
		// compute the combined status
		int updateDone = 1;
		for(String msg : iterCountMsgsMap.get(iterCountStr)) {
			updateDone *= Integer.parseInt(msg);
//			System.out.println("Updates: " + msg);
		}
		return (updateDone==0)?true:false;
	}
	
	public void disconnect() {
		channelHost.disconnect();
	}
}
