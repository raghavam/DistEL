package knoelab.classification.controller;

import java.util.Collections;

import redis.clients.jedis.Jedis;

/**
 * This class contains send() and receive() methods to
 * communicate with a channel. Note that there isn't a 
 * facility to subscribe/unsubscribe to channels. Assumes
 * a broadcast scenario where all channels are subscribed
 * by all the processes.
 * 
 * @author Raghava
 *
 */
public class CommunicationHandler2 {

	private Jedis channelHost;
	private int bufferSize;
	private String sendScript;
	private String receiveScript;
	
	public CommunicationHandler2(int bufferSize, int numSubscribers) {
		this.bufferSize = bufferSize;
		//TODO: read this from property file
		channelHost = new Jedis("nimbus2", 6479);
		channelHost.set("bufferSize", Integer.toString(bufferSize));
		
		// KEYS = {channel}, ARGV = {status}
		sendScript = "currentSize = redis.call('LLEN', KEYS[1]) " +
					 "maxSize = redis.call('GET', 'bufferSize') " +
					 "if(currentSize == maxSize) then " +				//channel is full, so block
					 	"sendWaitCounter = KEYS[1] .. 'SendWaitCnt' " +
					 	"redis.call('INCR', sendWaitCounter) " +
					 	"dummySendLst = KEYS[1] .. 'DummySendLst' " +
					 	"redis.call('BLPOP', dummySendLst, 0) " +
					 	"redis.call('LPUSH', KEYS[1], ARGV[1]) " +		//unblocked
					 "else " +											//add status to channel
					 	"redis.call('LPUSH', KEYS[1], ARGV[1]) " +
					 	"receiveWaitCounter = KEYS[1] .. 'ReceiveWaitCnt' " +
					 	"numReceivesBlocked = redis.call('GET', receiveWaitCounter) " +
					 	"receiveLst = KEYS[1] .. 'ReceiveWaitLst' " +
					 	"for i=1,numReceivesBlocked do " +				//unblock receivers on this channel
					 		"redis.call('LPUSH', receiveLst, 1) " +
					 	"end " +
					 "end ";
		
		receiveScript = "currentSize = redis.call('LLEN', KEYS[1]) " +
						"if(currentSize == 0) then " +					//channel is empty, so block
							"receiveWaitCounter = KEYS[1] .. 'ReceiveWaitCnt' " +
						 	"redis.call('INCR', receiveWaitCounter) " +
						 	"receiveLst = KEYS[1] .. 'ReceiveWaitLst' " +
						 	"redis.call('BLPOP', receiveLst, 0) " +
						 	"int status = redis.call('LPOP', KEYS[1]) " +  //unblocked
						 	"return status " +
						"else " +
							"int status = redis.call('LPOP', KEYS[1]) " +
							"sendWaitCounter = KEYS[1] .. 'SendWaitCnt' " +  
							"numSendersBlocked =  redis.call('GET', sendWaitCounter) " +
							"dummySendLst = KEYS[1] .. 'DummySendLst' " +
							"for i=1,numSendersBlocked do " +					// unblock senders
					 			"redis.call('LPUSH', dummySendLst, 1) " +
					 		"end " +
					 		"return status " +
						"end ";
	}
	
	public void send(String channel, int status) {
		channelHost.eval(sendScript, Collections.singletonList(channel), 
				Collections.singletonList(Integer.toString(status)));
	}
	
	public int receive(String channel) {
		int status = (Integer) channelHost.eval(receiveScript, 
							Collections.singletonList(channel), null);
		return status;
	}
	
	public void close() {
		channelHost.disconnect();
	}
}
