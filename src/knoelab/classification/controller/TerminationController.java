package knoelab.classification.controller;

import java.util.Iterator;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

import knoelab.classification.init.AxiomDistributionType;
import knoelab.classification.misc.Constants;
import knoelab.classification.misc.HostInfo;
import knoelab.classification.misc.PropertyFileHandler;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPubSub;

/**
 * This class(TC) keeps track of the messages sent
 * by Classifiers on each machine and decides
 * the termination of Classifier which in turn
 * terminate the Classification process.
 * 
 * @author Raghava
 *
 */
@Deprecated //class no longer in use
public class TerminationController extends JedisPubSub {

	private LinkedBlockingQueue<String> noUpdateMsgQueue;
	private PropertyFileHandler propertyFileHandler;
	private String classifierChannel;
	private String msgSeparator;
	private ExecutorService threadExecutor;
	
	public TerminationController() {
		noUpdateMsgQueue = new LinkedBlockingQueue<String>(Constants.NUM_RULE_TYPES);
		propertyFileHandler = PropertyFileHandler.getInstance();
		classifierChannel = propertyFileHandler.getClassifierChannel();
//		msgSeparator = propertyFileHandler.getExistentialAxiomSeparator();
		threadExecutor = Executors.newSingleThreadExecutor();
	}
	
	@Override
	public void onMessage(String channel, String message) {
		// message contains the following:
		// IP address of the machine from which the message was sent
		// CompletionRule Type -- (Type1 to Type4)
		// UPDATE or NO_UPDATE 
		System.out.println("Message received: " + message);
		threadExecutor.execute(new MessageProcessor(message, classifierChannel, 
												this, msgSeparator));
	}

	@Override
	public void onPMessage(String arg0, String arg1, String arg2) { }
	@Override
	public void onPSubscribe(String arg0, int arg1) { }
	@Override
	public void onPUnsubscribe(String arg0, int arg1) { }
	@Override
	public void onSubscribe(String arg0, int arg1) { }

	@Override
	public void onUnsubscribe(String arg0, int arg1) { 
		threadExecutor.shutdown();
		System.out.println("Unsubscribing....");  
	}
	
	public int addToNoUpdateQueue(String machineIPAddr) {
		noUpdateMsgQueue.add(machineIPAddr);
		return noUpdateMsgQueue.remainingCapacity();
	}
	
	public boolean removeFromNoUpdateQueue(String machineIPAddr) {
		return noUpdateMsgQueue.remove(machineIPAddr);
	}
	
	public void sendTerminateMsgAndUnsubscribe() {
		Iterator<String> queueIterator = noUpdateMsgQueue.iterator();
		while(queueIterator.hasNext()) {
			Jedis jedis = new Jedis(queueIterator.next(), Constants.JEDIS_DEFAULT_PORT);
			jedis.publish(classifierChannel, ResponseMessage.TERMINATE.getMessageCode());
			jedis.disconnect();
		}
		unsubscribe();
	}
	
	public static void main(String[] args) {
		PropertyFileHandler propertyFileHandler = PropertyFileHandler.getInstance();
		String channel = propertyFileHandler.getTerminationControllerChannel();
		HostInfo localHostInfo = propertyFileHandler.getLocalHostInfo();
		System.out.println("Starting TC...");
		Jedis jedisPubSubListener = new Jedis(localHostInfo.getHost(), localHostInfo.getPort());
		jedisPubSubListener.subscribe(new TerminationController(), channel);
		
		jedisPubSubListener.disconnect();
		System.out.println("Exiting TC");
	}
}

class MessageProcessor implements Runnable {
	
	private String message;
	private String channel;
	private TerminationController terminationController;
	private String msgSeparator;

	MessageProcessor(String message, String channel, 
			TerminationController terminationController, String msgSeparator) {
		this.message = message;
		this.channel = channel;
		this.terminationController = terminationController;
		this.msgSeparator = msgSeparator;
	}
	
	@Override
	public void run() {
		try{
			String[] msgFragments = message.split(msgSeparator);
			AxiomDistributionType axiomType = null;
//								convertCodeToMessage(msgFragments[1]);
			ResponseMessage responseMessage = ResponseMessage.
								convertCodeToMessage(msgFragments[2]);
			
			switch(responseMessage) {
			case NO_UPDATE:	int remainingCapacity = terminationController.
										addToNoUpdateQueue(msgFragments[0]); 
							if(remainingCapacity == 0)
								terminationController.sendTerminateMsgAndUnsubscribe();
							break;
							
			case UPDATE:		switch(axiomType) {
							case CR_TYPE1_1:	// send CONTINUE msg to type1, type2, type3 nodes
											break;
							case CR_TYPE2:	// send CONTINUE msg to type3, type4 nodes
											break;
							case CR_TYPE3_1:	// send CONTINUE msg to type1, type2, type3 nodes
											break;
							case CR_TYPE4:	// send CONTINUE msg to type3, type4 nodes
											break;
							default:	throw new Exception("Unexpected Axiom type: " + axiomType);				
							}
							break;
			default: throw new Exception("Unexpected message type: " + responseMessage);				
			}
		}
		catch(Exception e) {
			e.printStackTrace();
		}
	}
	
}
