package knoelab.classification;

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
	
	public ELClassifier() {
		try {
			propertyFileHandler = PropertyFileHandler.getInstance();
			HostInfo localHostInfo = propertyFileHandler.getLocalHostInfo();
			localStore = new Jedis(localHostInfo.getHost(), 
					localHostInfo.getPort(), Constants.INFINITE_TIMEOUT);
			
			String getAndIncrementScript = 
				"local keyExists = redis.call('EXISTS', 'NUM_JOBS') " +
				"local numJobs = 0 " +
				"if(keyExists == 0) then " +
					"redis.call('SET', 'NUM_JOBS', 1) " +
					"return numJobs " +
				"else " +
					"numJobs = redis.call('GET', 'NUM_JOBS') " +
					"redis.call('INCR', 'NUM_JOBS') " +
					"return tonumber(numJobs) " +
				"end ";
			localStore.connect();
			Long numJobs = 
					(Long)localStore.eval(getAndIncrementScript);		
			port = localHostInfo.getPort() + (10 * numJobs.intValue());
			ruleStore = new Jedis(localHostInfo.getHost(), 
								port, Constants.INFINITE_TIMEOUT);			
			String ruleType = ruleStore.get(propertyFileHandler.getAxiomTypeKey());
			axiomType = AxiomDistributionType.valueOf(ruleType);
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
			switch(axiomType) {
			
			case CR_TYPE1_1:  Type1_1AxiomProcessor axiomProcessor11 = 
										new Type1_1AxiomProcessor(port);
							  axiomProcessor11.processRules();
							  break;
			case CR_TYPE1_2: Type1_2AxiomProcessor axiomProcessor12 = 
										new Type1_2AxiomProcessor(port);
							 axiomProcessor12.processRules();
							 break;				
			case CR_TYPE2:	Type2AxiomProcessor axiomProcessor2 = 
										new Type2AxiomProcessor(port);
							axiomProcessor2.processRules();
							break;			
			case CR_TYPE3_1:	Type3_1AxiomProcessor axiomProcessor3_1 = 
										new Type3_1AxiomProcessor(port);
							axiomProcessor3_1.processRules();
							break;
			case CR_TYPE3_2:Type3_2AxiomProcessor axiomProcessor3_2 = 
										new Type3_2AxiomProcessor(port);
							axiomProcessor3_2.processRules();
							break;
			case CR_TYPE4:	Type4AxiomProcessor axiomProcessor4 = 
										new Type4AxiomProcessor(port);
							axiomProcessor4.processRules();
							break;
			case CR_TYPE5:	Type5AxiomProcessor axiomProcessor5 = 
										new Type5AxiomProcessor(port);
							axiomProcessor5.processRules();
							break;					
			}
			Util.printElapsedTime(start);
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


