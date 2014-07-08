package knoelab.classification.worksteal;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;

import knoelab.classification.misc.Constants;
import knoelab.classification.misc.HostInfo;
import knoelab.classification.misc.PropertyFileHandler;
import knoelab.classification.misc.ScoreComparator;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPubSub;

public class ProgressMessageHandler extends JedisPubSub implements Runnable {

	private Jedis subscriberJedis;
	private CountDownLatch latch;
	//key -> IterationNumber, <MachineName:Port:Type> , value -> %completed
	private ConcurrentHashMap<String, Map<String, Double>> iterTypeScoreMap;
	private int totalMsgCount;
	private static int currentCount = 0;
	private CountDownLatch barrierSynch;
	private CountDownLatch progressMsgLatch;
	
	public ProgressMessageHandler(CountDownLatch waitLatch, 
			CountDownLatch barrierSynch, CountDownLatch progressMsgLatch) {
		this.latch = waitLatch;
		this.progressMsgLatch = progressMsgLatch;
		iterTypeScoreMap = new ConcurrentHashMap<String, Map<String, Double>>();
		PropertyFileHandler propertyFileHandler = PropertyFileHandler.getInstance();
		HostInfo channelHostInfo = propertyFileHandler.getChannelHost();
		subscriberJedis = new Jedis(channelHostInfo.getHost(), 
				channelHostInfo.getPort(), Constants.INFINITE_TIMEOUT);
		totalMsgCount = propertyFileHandler.getNodeCount();
		this.barrierSynch = barrierSynch;
	}
	
	@Override
	public void run() {
		System.out.println("About to start ProgressMsgHandler...");
		subscriberJedis.subscribe(this, Constants.PROGRESS_CHANNEL);
		subscriberJedis.disconnect();
		latch.countDown();
	}		
	
	@Override
	public void onUnsubscribe(String channel, int subscribedChannels) {
		System.out.println("Unsubscribed...");
	}
	
	@Override
	public void onSubscribe(String channel, int subscribedChannels) {
		System.out.println("Subscribed to channel: " + channel);
		barrierSynch.countDown();
	}
	
	@Override
	public void onPUnsubscribe(String arg0, int arg1) { }			
	@Override
	public void onPSubscribe(String arg0, int arg1) { }			
	@Override
	public void onPMessage(String arg0, String arg1, String arg2) {	}
	
	@Override
	public void onMessage(String channel, String message) {
//		System.out.println("Received message: " + message);
		String[] iterTypeScoreStr = message.split(Constants.SEPARATOR_RATE);
		Map<String, Double> typeScoreMap = iterTypeScoreMap.get(iterTypeScoreStr[0]);
		Double currentProgress = Double.parseDouble(iterTypeScoreStr[2]);		
		if(typeScoreMap == null) {
			typeScoreMap = new HashMap<String, Double>();
			typeScoreMap.put(iterTypeScoreStr[1], currentProgress);
			iterTypeScoreMap.put(iterTypeScoreStr[0], typeScoreMap);
			currentCount++;
		}
		else {
			Double prevProgress = typeScoreMap.get(iterTypeScoreStr[1]);
			if(prevProgress == null) {
				typeScoreMap.put(iterTypeScoreStr[1], currentProgress);
				currentCount++;
//				System.out.println(currentCount + "   " + message);
				if(currentCount == totalMsgCount) {
					currentCount = 0;
					System.out.println("Resetting currentCount and sending done msg");
					//using a latch works because, it should also wait for its local progress msg
					progressMsgLatch.countDown();
				}
			}
			else
				if(currentProgress > prevProgress)
					typeScoreMap.put(iterTypeScoreStr[1], currentProgress);
		}
//		System.out.println("currentCount: " + currentCount);
	}
	
	public String getLeastCompletedType(int iterationNum) {
		String keyToSort = Integer.toString(iterationNum);
		//sort the map
		List<Entry<String, Double>> entryList = 
			new ArrayList<Entry<String, Double>>(
					iterTypeScoreMap.get(keyToSort).entrySet());
		Collections.sort(entryList, new ScoreComparator<Double>(true));
		
//		for(Entry<String, Double> entry : entryList)
//			System.out.print(entry.getKey() + ", " + entry.getValue() + "   ");
//		System.out.println();
		
		//check of all of them are 1.0 i.e. completed, then return null
		for(Entry<String, Double> entry : entryList)
			if(entry.getValue().doubleValue() != 1.0)
				return entry.getKey();
		return null;
	}
	
	public void resetLatchAndClearMessages(
			CountDownLatch progressMsgLatch, int iterationNum) {
		this.progressMsgLatch = progressMsgLatch;
		String keyToDelete = Integer.toString(iterationNum);
		iterTypeScoreMap.remove(keyToDelete);
	}
}




