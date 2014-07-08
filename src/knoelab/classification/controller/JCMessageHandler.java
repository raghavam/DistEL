package knoelab.classification.controller;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPubSub;
import redis.clients.jedis.Transaction;

public class JCMessageHandler implements Runnable {

//	private TestTypeController testTypeController;
	private String channel;
	private CountDownLatch latch;
/*	
	JCMessageHandler(TestTypeController tc, String channel, 
			CountDownLatch latch) {
//		testTypeController = tc;
		this.channel = channel;
		this.latch = latch;
	}
*/	
	public void run() {
		Jedis jedisPubSubListener = new Jedis("localhost", 6479);
		jedisPubSubListener.subscribe(new PubSubTC(), channel);
	}

class PubSubTC extends JedisPubSub {

	private int machineCount = 0;
	// 0 - there was an update. 1 - no update
	private int updateDone;
	private Jedis barrierHost;
	private String barrierSynch;
	private Map<String, Integer> hostStatusMap;
	
	PubSubTC() {
		updateDone = 1;
		hostStatusMap = new HashMap<String, Integer>();
		barrierHost = new Jedis("nimbus2", 6479);
		barrierHost.connect();
		
		barrierSynch = "totalProcesses = redis.call('GET', 'TotalProcesses') " +
					   "totalProcesses = totalProcesses-1 " +
					   "if(totalProcesses == 0) then " +
					   		"total = redis.call('GET', 'NumBlockedProcesses') " +
					   		"for i=1,total do " +
					   			"redis.call('LPUSH', 'DummyLst', 1) " +
					   		"end " +
					   	"else " +
					   		"redis.call('SET', 'TotalProcesses', totalProcesses) " +
					   		"redis.call('INCR', 'NumBlockedProcesses') " +
//					   		"redis.call('BLPOP', 'DummyLst', 0) " +
					   "end " +
					   "return totalProcesses ";
	}
	
	@Override
	public void onMessage(String channel, String message) {
		try {
			machineCount++;
			String[] split = message.split("~");
			System.out.println("msg received: " + message + "   " + split[1]);
			if(hostStatusMap.containsKey(split[0])) {
				//TODO: should I be concerned with the earliest msg from same 2 processes
				System.out.println("Msg from this node already exists: " + split[0]);
			}
			else {
				int status = Integer.parseInt(split[1]);
				hostStatusMap.put(split[0], status);
				updateDone *= status;
			}
			if(machineCount == 5) {
				hostStatusMap.clear();
				machineCount = 0;
				if(updateDone == 1) {
					// terminate;
					System.out.println("Terminating TypeController...");
					unsubscribe();
				}
				else {
					// continue with next iteration;
					System.out.println("Calling processRules()... " + updateDone);
					updateDone = 1;
//					testTypeController.processRules();
				}
			}
		}
		catch(Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public void onPMessage(String pattern, String channel, String message) { }
	@Override
	public void onPSubscribe(String pattern, int subscribedChannels) { }
	@Override
	public void onPUnsubscribe(String pattern, int subscribedChannels) { }
	@Override
	public void onSubscribe(String channel, int subscribedChannels) { 
		System.out.println("Subscribed...");
		Long currentProcesses = (Long) barrierHost.eval(barrierSynch);
		System.out.println("Return value: " + currentProcesses);
		if(currentProcesses > 0) {
			System.out.println("Blocking on DummyLst");
			barrierHost.blpop(0, "DummyLst");
		}
		latch.countDown();
		barrierHost.disconnect();
	}
	@Override
	public void onUnsubscribe(String channel, int subscribedChannels) { }
	}	
}