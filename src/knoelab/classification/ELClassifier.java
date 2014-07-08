package knoelab.classification;

import java.net.InetAddress;
import java.util.GregorianCalendar;

import knoelab.classification.init.AxiomDistributionType;
import knoelab.classification.misc.Constants;
import knoelab.classification.misc.HostInfo;
import knoelab.classification.misc.PropertyFileHandler;
import knoelab.classification.misc.Util;
import redis.clients.jedis.Jedis;

public class ELClassifier {

	private PropertyFileHandler propertyFileHandler;
	private Jedis localStore;
	private Jedis ruleStore;
	private AxiomDistributionType axiomType;
	private int port;
	private boolean foundBottom;
	
	public ELClassifier() {
		try {
			propertyFileHandler = PropertyFileHandler.getInstance();
			HostInfo localHostInfo = propertyFileHandler.getLocalHostInfo();
			localStore = new Jedis(localHostInfo.getHost(), 
					localHostInfo.getPort(), Constants.INFINITE_TIMEOUT);
			
			String getAndIncrementScript = 
				"local keyExists = redis.call('EXISTS', '" + Constants.NUM_JOBS + "') " +
				"local numJobs = 0 " +
				"if(keyExists == 0) then " +
					"redis.call('SET', '" + Constants.NUM_JOBS + "', 1) " +
					"return numJobs " +
				"else " +
					"numJobs = redis.call('GET', '" + Constants.NUM_JOBS + "') " +
					"redis.call('INCR', '" + Constants.NUM_JOBS + "') " +
					"return tonumber(numJobs) " +
				"end ";
			localStore.connect();
			Long numJobs = 
					(Long)localStore.eval(getAndIncrementScript);		
			port = localHostInfo.getPort() + numJobs.intValue();
			ruleStore = new Jedis(localHostInfo.getHost(), 
								port, Constants.INFINITE_TIMEOUT);			
			String ruleType = ruleStore.get(propertyFileHandler.getAxiomTypeKey());
			axiomType = AxiomDistributionType.valueOf(ruleType);
			String foundBottomStr = ruleStore.get(Constants.FOUND_BOTTOM);
			if((foundBottomStr == null) || foundBottomStr.equals("0"))
				foundBottom = false;
			else if((foundBottomStr != null) && foundBottomStr.equals("1"))
				foundBottom = true;
		}
		catch(Exception e) {
			e.printStackTrace();
		}
		finally {
			if(localStore != null)
				localStore.disconnect();
			if(ruleStore != null)
				ruleStore.disconnect();
		}
	}
	
	public void classify() throws Exception {
		try{
			GregorianCalendar start = new GregorianCalendar();
			String machineName = null;
			try {
				machineName = InetAddress.getLocalHost().getHostName();
			}
			catch(Exception e) { e.printStackTrace(); }
			System.out.println(axiomType.toString());
			switch(axiomType) {
			
			case CR_TYPE1_1:  Type1_1AxiomProcessor axiomProcessor11 = 
									new Type1_1AxiomProcessor(machineName, port);
							  axiomProcessor11.processRules();
							  break;
			case CR_TYPE1_2: Type1_2AxiomProcessor axiomProcessor12 = 
									new Type1_2AxiomProcessor(machineName, port);
							 axiomProcessor12.processRules();
							 break;				
			case CR_TYPE2:	Type2AxiomProcessor axiomProcessor2 = 
									new Type2AxiomProcessor(machineName, port);
							axiomProcessor2.processRules();
							break;			
			case CR_TYPE3_1:	Type3_1AxiomProcessor axiomProcessor3_1 = 
									new Type3_1AxiomProcessor(machineName, port);
							axiomProcessor3_1.processRules();
							break;
			case CR_TYPE3_2:Type3_2AxiomProcessor axiomProcessor3_2 = 
									new Type3_2AxiomProcessor(machineName, port);
							axiomProcessor3_2.processRules();
							break;
			case CR_TYPE4:	Type4AxiomProcessor axiomProcessor4 = 
									new Type4AxiomProcessor(machineName, port);
							axiomProcessor4.processRules();
							break;
			case CR_TYPE5:	Type5AxiomProcessor axiomProcessor5 = 
									new Type5AxiomProcessor(machineName, port);
							axiomProcessor5.processRules();
							break;					
			case CR_TYPE_BOTTOM: if(foundBottom) {
									TypeBottomAxiomProcessor bottomAxiomProcessor 
									= new TypeBottomAxiomProcessor(machineName, port);
									bottomAxiomProcessor.processRules();
								 }
								 break;
			default: throw new Exception("Unknown type: " + axiomType.toString());					 
			}
			System.out.println("Time taken (millis): " + 
					Util.getElapsedTime(start));
		}
		finally {
//			terminationController.disconnect();
		}
	}

	public static void main(String[] args) throws Exception {
		ELClassifier classifier = new ELClassifier();
		classifier.classify();
	}

}


