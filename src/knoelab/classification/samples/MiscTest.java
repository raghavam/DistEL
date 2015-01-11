package knoelab.classification.samples;

import java.io.File;
import java.io.FileInputStream;
import java.io.UnsupportedEncodingException;
import java.net.InetAddress;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Random;
import java.util.Scanner;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.semanticweb.elk.owlapi.ElkReasonerFactory;
import org.semanticweb.owlapi.apibinding.OWLManager;
import org.semanticweb.owlapi.io.OWLFunctionalSyntaxOntologyFormat;
import org.semanticweb.owlapi.io.OWLXMLOntologyFormat;
import org.semanticweb.owlapi.model.AxiomType;
import org.semanticweb.owlapi.model.IRI;
import org.semanticweb.owlapi.model.OWLAxiom;
import org.semanticweb.owlapi.model.OWLClass;
import org.semanticweb.owlapi.model.OWLClassExpression;
import org.semanticweb.owlapi.model.OWLDataHasValue;
import org.semanticweb.owlapi.model.OWLDataProperty;
import org.semanticweb.owlapi.model.OWLDataPropertyExpression;
import org.semanticweb.owlapi.model.OWLDatatype;
import org.semanticweb.owlapi.model.OWLLogicalAxiom;
import org.semanticweb.owlapi.model.OWLObjectProperty;
import org.semanticweb.owlapi.model.OWLObjectPropertyExpression;
import org.semanticweb.owlapi.model.OWLObjectPropertyRangeAxiom;
import org.semanticweb.owlapi.model.OWLObjectSomeValuesFrom;
import org.semanticweb.owlapi.model.OWLOntology;
import org.semanticweb.owlapi.model.OWLOntologyCreationException;
import org.semanticweb.owlapi.model.OWLOntologyManager;
import org.semanticweb.owlapi.model.OWLSubClassOfAxiom;
import org.semanticweb.owlapi.reasoner.InferenceType;
import org.semanticweb.owlapi.reasoner.OWLReasoner;
import org.semanticweb.owlapi.reasoner.OWLReasonerFactory;
import org.semanticweb.owlapi.util.OWLOntologyMerger;
import org.semanticweb.owlapi.util.SimpleIRIMapper;

import com.clarkparsia.pellet.owlapiv3.PelletReasoner;
import com.clarkparsia.pellet.owlapiv3.PelletReasonerFactory;

import knoelab.classification.init.AxiomDistributionType;
import knoelab.classification.init.Normalizer;
import knoelab.classification.misc.AxiomDB;
import knoelab.classification.misc.Constants;
import knoelab.classification.misc.HostInfo;
import knoelab.classification.misc.PropertyFileHandler;
import knoelab.classification.misc.ScoreComparator;
import knoelab.classification.misc.ScriptsCollection;
import knoelab.classification.misc.Util;
import knoelab.classification.pipeline.PipelineManager;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisMonitor;
import redis.clients.jedis.JedisPubSub;
import redis.clients.jedis.JedisShardInfo;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Protocol;
import redis.clients.jedis.Response;
import redis.clients.jedis.ShardedJedis;
import redis.clients.jedis.Transaction;
import redis.clients.jedis.Tuple;
import redis.clients.util.Hashing;
import redis.clients.util.SafeEncoder;
import uk.ac.manchester.cs.factplusplus.owlapiv3.FaCTPlusPlusReasonerFactory;

/**
 * Class used to test any random thing in the system
 * 
 * @author Raghava
 *
 */
public class MiscTest {

	public static void main(String[] args) throws Exception {			
		
		// this only works if "java -cp dist/.jar class" format is used
//		PropertyFileHandler p = PropertyFileHandler.getInstance();
		
//		checkTransactions();
//		initAxiomTypes();
//		convertToFunctionalSyntax(args[0]);
//		testMultiZRangeByScore();
//		printOntologyStats(args[0]);
//		testLuaTable();
//		compareReasoners(args[0]);
//		testZRangeByScore();
//		suppressNewScoreZAdd(); 
//		printAxioms(args[0]);
//		testLua();
//		testRedisScripting();
//		testSMembers();
//		testPipelineReadType3_1();
//		testPipelineReadType3_2();
//		testSortedSetMethods();
//		testNullVals();
//		printKeyCount();
//		getMaxSuperClassCount();
//		testSetContainment();
//		testUnionStoreAlt();
//		testLuaZRange();
//		testRoleChainLoad2();		
//		testConnection();
//		testLoadPropFile();
//		testNamedIndividual();
//		testConnection(args);
//		testMonitorCommands();
//		testOntologyMerger(args);		
//		testPubSub();		
//		removeNonELAxioms(args[0]);
//		checkObjectPropertyRangeAxioms(args[0]);
//		testPipelineEval();
//		testReadWriteRedisSpeed();
//		testJedisShardingWrite();
//		testJedisShardingRead();
		
		testScoreComparator();
	}
	
	private static void testScoreComparator() {
		Map<String, Double> map = new HashMap<String, Double>();
		map.put("one", 1.23);
		map.put("two", 2.33);
		map.put("three", 0.23);
		map.put("four", 3.13);
		map.put("five", 3.03);
		map.put("six", 0.13);
		map.put("seven", 4.13);
		map.put("eight", -1.43);
		map.put("nine", 5.53);
		map.put("ten", -0.83);
		map.put("NaN", (double)0/0);
		List<Entry<String, Double>> entryList = 
				new ArrayList<Entry<String, Double>>(map.entrySet());
		Collections.sort(entryList, new ScoreComparator());
		for(Entry<String, Double> entry : entryList)
			System.out.println(entry.getKey() + "   " + entry.getValue());
		
		List<Entry<String, Double>> reverseEntryList = 
				new ArrayList<Entry<String, Double>>(map.entrySet());
		Collections.sort(reverseEntryList, 
				Collections.reverseOrder(new ScoreComparator()));
		System.out.println("\n");
		for(Entry<String, Double> entry : reverseEntryList)
			System.out.println(entry.getKey() + "   " + entry.getValue());
	}
	
	private static void testJedisShardingWrite() {
		//sharding not giving proper results currently
		List<JedisShardInfo> type5Shards = new ArrayList<JedisShardInfo>();
		type5Shards.add(new JedisShardInfo("nimbus5", 6379, 
				Constants.INFINITE_TIMEOUT));
		type5Shards.add(new JedisShardInfo("nimbus11", 6379, 
				Constants.INFINITE_TIMEOUT));
		ShardedJedis type5ShardedJedis = new ShardedJedis(type5Shards, 
				Hashing.MURMUR_HASH);
		type5ShardedJedis.sadd("02192", "0100", "0110");
		type5ShardedJedis.sadd("02182", "0100", "0110");
		type5ShardedJedis.close();
	}
	
	private static void testJedisShardingRead() {
		Jedis jedis = new Jedis("nimbus11", 6379, Constants.INFINITE_TIMEOUT);
		List<JedisShardInfo> type5Shards = new ArrayList<JedisShardInfo>();
		type5Shards.add(new JedisShardInfo("nimbus5", 6379, 
				Constants.INFINITE_TIMEOUT));
		type5Shards.add(new JedisShardInfo("nimbus11", 6379, 
				Constants.INFINITE_TIMEOUT));
		ShardedJedis type5ShardedJedis = new ShardedJedis(type5Shards, 
				Hashing.MURMUR_HASH);
		Set<String> result = jedis.smembers("02192");
		System.out.println("02192 from jedis: " + result);
		result = type5ShardedJedis.smembers("02192");
		System.out.println("02192 from ShardedJedis: " + result);
		jedis.close();
		type5ShardedJedis.close();
	}
	
	private static void testReadWriteRedisSpeed() {
		int max = 100000;
		PropertyFileHandler propertyFileHandler = 
				PropertyFileHandler.getInstance();
		boolean isLocal = false;
		HostInfo hostInfo;
		Jedis store;
		if(isLocal) 
			hostInfo = propertyFileHandler.getLocalHostInfo();
		else 
			hostInfo = new HostInfo("nimbus2", 6379);
		store = new Jedis(hostInfo.getHost(), hostInfo.getPort());
		store.flushAll();
		PipelineManager pipelineManager = new PipelineManager(
				Collections.singletonList(hostInfo), 
				propertyFileHandler.getPipelineQueueSize());
		System.out.println("Using 100000 keys for sorted set with " +
				"100 values in each key");
		long readStartTime = System.nanoTime();
		testReadSpeed(max, pipelineManager, hostInfo);
		long readEndTime = System.nanoTime();
		double diff = (readEndTime - readStartTime)/(double)1000000000;
		System.out.println("Time taken for local write (secs) : " + diff);
		
		readStartTime = System.nanoTime();
		testWriteSpeed(max, pipelineManager, hostInfo);
		readEndTime = System.nanoTime();
		diff = (readEndTime - readStartTime)/(double)1000000000;
		System.out.println("Time taken for local read (secs) : " + diff);
		
		store.close();
	}
	
	private static void testReadSpeed(int max, 
			PipelineManager pipelineManager, HostInfo hostInfo) {
		for(int i = 1; i <= max; i++) {
			for(int j = 1; j <= 100; j++)
				pipelineManager.pzadd(hostInfo, Integer.toString(i), 1.0, 
						Integer.toString(j), AxiomDB.NON_ROLE_DB);
		}
		pipelineManager.synchAll(AxiomDB.NON_ROLE_DB);
		pipelineManager.resetSynchResponse();
	}
	
	private static void testWriteSpeed(int max, 
			PipelineManager pipelineManager, HostInfo hostInfo) {
		for(int i = 1; i <= max; i++) {
			pipelineManager.pZRangeByScoreWithScores(hostInfo, 
					Integer.toString(i), Double.NEGATIVE_INFINITY, 
					Double.POSITIVE_INFINITY, AxiomDB.NON_ROLE_DB);
		}
		pipelineManager.synchAndCloseAll(AxiomDB.NON_ROLE_DB);
		pipelineManager.resetSynchResponse();
	}
	
	private static void testPipelineEval() {
		
		String scriptSingleConcept =  
				"local elementScore = redis.call('ZRANGE', " +
					"KEYS[1], -1, -1, 'WITHSCORES') " +
				"local unique = 0 " +
				"local minScore = table.remove(ARGV) " +
				"local toBeAddedList = redis.call(" +
					"'ZRANGEBYSCORE', KEYS[1], minScore, '+inf') " +
				"local escore " +
				"local score " +
				"local ret " +
				"local unique = 0 " +
				"for index1,value1 in pairs(ARGV) do " +
					"escore = redis.call('ZRANGE', value1, -1, -1, 'WITHSCORES') " +
					"score = escore[2] + " + Constants.SCORE_INCREMENT + " " +
					"for index2,value2 in pairs(toBeAddedList) do " +
						"if(not redis.call('ZSCORE', value1, value2)) then " +
							"ret = redis.call('ZADD', value1, score, value2) " +
							"unique = unique + ret " +
						"end " +
					"end " +
				"end " +
				"return tostring(elementScore[2]) .. ':' .. tostring(unique) ";
		
		String script = "local t = {} " +
						"t[1] = 'a' " +
						"t[2] = 'b' " +
						"return t ";
		
		PropertyFileHandler propertyFileHandler = 
				PropertyFileHandler.getInstance();
		HostInfo resultHostInfo = propertyFileHandler.getResultNode();
		Jedis jedis = new Jedis(resultHostInfo.getHost(), 
				resultHostInfo.getPort(), Constants.INFINITE_TIMEOUT);
		Pipeline p = jedis.pipelined();
		Response<String> response = p.eval(script, 
				Collections.singletonList("062016890"), 
				Collections.singletonList("05595490"));
		p.sync();
		response.get();
		String[] responseStr = response.get().split(":");
		System.out.println(response.get());
		System.out.println(responseStr[0] + "  " + responseStr[1]);
/*		
		List<Object> responseList = p.syncAndReturnAll();
		jedis.disconnect();
		System.out.println("is responseList null? " + (responseList==null));
		for(Object response : responseList) {
			ArrayList<String> nextMinScoreList = 
					(ArrayList<String>) response;
			double nextMinScore = Double.parseDouble(nextMinScoreList.get(0));	
			Long numUpdates = Long.parseLong(nextMinScoreList.get(1));
			System.out.println("nextMinScore: " + nextMinScore);
			System.out.println("numUpdates: " + numUpdates);
		}
*/		
	}
	
	private static void testFactPlusPlus(String file) throws Exception {
		File ontFile = new File(file);
		IRI documentIRI = IRI.create(ontFile);
		OWLOntologyManager manager = OWLManager.createOWLOntologyManager();	
		OWLOntology ontology = manager.loadOntologyFromOntologyDocument(documentIRI);
		GregorianCalendar start = new GregorianCalendar();
		System.out.println("Using Fact++");
		OWLReasoner reasoner = new FaCTPlusPlusReasonerFactory().createReasoner(ontology);
		reasoner.precomputeInferences(
				InferenceType.CLASS_HIERARCHY);
		reasoner.dispose();
		System.out.println("Time taken (millis): " + 
				Util.getElapsedTime(start));
	}
	
	private static void checkObjectPropertyRangeAxioms(String ontFile) 
			throws Exception {
		OWLOntologyManager manager = OWLManager.createOWLOntologyManager();
        File owlFile = new File(ontFile);
        IRI documentIRI = IRI.create(owlFile);
        OWLOntology ontology = manager.loadOntologyFromOntologyDocument(
        							documentIRI);
        Set<OWLObjectPropertyRangeAxiom> objPropRangeAxioms = 
        		ontology.getAxioms(AxiomType.OBJECT_PROPERTY_RANGE);
        Map<OWLObjectPropertyExpression, Set<OWLClassExpression>> 
        	objPropRangeMap = new HashMap<OWLObjectPropertyExpression, 
        	Set<OWLClassExpression>>();
		for(OWLObjectPropertyRangeAxiom ax : objPropRangeAxioms) {
			Set<OWLClassExpression> rangeSet = 
					objPropRangeMap.get(ax.getProperty());
			if(rangeSet == null) {
				rangeSet = new HashSet<OWLClassExpression>();
				rangeSet.add(ax.getRange());
				objPropRangeMap.put(ax.getProperty(), rangeSet);
			}
			else
				rangeSet.add(ax.getRange());
		}
		Set<OWLObjectPropertyExpression> keys = objPropRangeMap.keySet();
		System.out.println("Number of range axioms: " + objPropRangeMap.size());
		Set<OWLSubClassOfAxiom> axioms = ontology.getAxioms(AxiomType.SUBCLASS_OF);
		System.out.println("SubClass axioms: " + axioms.size());
		int count1 = 0, count2 = 0;
		for(OWLSubClassOfAxiom ax : axioms) {
			OWLClassExpression oce = ax.getSuperClass();
			if(oce instanceof OWLObjectSomeValuesFrom) {
				count1++;
				OWLObjectSomeValuesFrom osv = (OWLObjectSomeValuesFrom) oce;
				if(objPropRangeMap.containsKey(osv.getProperty()))
					count2++;
			}
		}
		System.out.println("Number of existential axioms: " + count1);
		System.out.println("Number of existential range axioms: " + count2);
	}
	
	private static void removeNonELAxioms(String ontPath) throws Exception {
		OWLOntologyManager manager = OWLManager.createOWLOntologyManager();
        File owlFile = new File(ontPath);
        IRI documentIRI = IRI.create(owlFile);
        OWLOntology ontology = manager.loadOntologyFromOntologyDocument(
        							documentIRI);
        Set<OWLLogicalAxiom> axioms = ontology.getLogicalAxioms();
        System.out.println("Logical Axioms: " + axioms.size());
        Set<OWLDatatype> dataTypes = new HashSet<OWLDatatype>();
        boolean isRemoved = false;
        long count = 0;
        for(OWLLogicalAxiom axiom : axioms) {
        	if(axiom instanceof OWLSubClassOfAxiom) {
        		OWLClassExpression oce = ((OWLSubClassOfAxiom)axiom).getSuperClass();
        		if(oce instanceof OWLDataHasValue) {
        			OWLDatatype dataType = 
        					((OWLDataHasValue)oce).getValue().getDatatype();
        			manager.removeAxiom(ontology, axiom);
        			dataTypes.add(dataType);
        			count++;
        			if(dataType.toString().equals("xsd:gMonth")) {
        				System.out.println(axiom);
//        				manager.removeAxiom(ontology, axiom);
//       				isRemoved = true;
        			}
        		}
        	}
        }
//        if(isRemoved)
//        	manager.saveOntology(ontology, IRI.create(
//        			new File("base-ont-without-datatypes.owl")));
        System.out.println("DataTypes: " + dataTypes);
        System.out.println("No. of axioms with data types: " + count);
	}
	
	private static void testPubSub() throws Exception {
		Jedis listener = new Jedis("localhost", 6479);
		CountDownLatch latch = new CountDownLatch(1);
		ExecutorService executor = Executors.newSingleThreadExecutor();
		PubSubHandler pubSubHandler = new PubSubHandler(listener, latch);
		executor.execute(pubSubHandler);
		System.out.println("Started the thread");
		String channel = "progress";
		Jedis publisher = new Jedis("localhost", 6479);
		publisher.publish(channel, "TYPE1_2:0.65");
		publisher.publish(channel, "TYPE1_1:0.56");
		publisher.publish(channel, "TYPE3_2:0.45");
		publisher.publish(channel, "TYPE2:0.77");
		publisher.publish(channel, "DONE");
		
		pubSubHandler.unsubscribe();
		executor.shutdown();
		latch.await();
		if(executor.isTerminated())
			executor.awaitTermination(5, TimeUnit.SECONDS);
		if(executor.isTerminated())
			System.out.println("All tasks completed now");
		System.out.println("Disconnecting...");
		listener.disconnect();
		publisher.disconnect();
	}
	
	private static void testMonitorCommands() {
		Jedis jedis = new Jedis("localhost", 6479);
		jedis.monitor(new JedisMonitor() {
			
			@Override
			public void onCommand(String command) {
				System.out.println(command);
			}
		});
	}
	
	private static void testOntologyMerger(String[] args) throws Exception {
		/*If its first time, then use the merged version since other 3 
		 * ontologies (Time, Core, TravelTime) are required. After that, use the
		 * mapper version -- sufficient to just refer the imports and not load
		 * them.
		 */
		
		OWLOntologyManager manager = OWLManager.createOWLOntologyManager();
		SimpleIRIMapper iriMapperTime = new SimpleIRIMapper(
				IRI.create("http://www.w3.org/2006/time"), 
				IRI.create(new File("Time.owl")));
		manager.addIRIMapper(iriMapperTime);
/*		
		SimpleIRIMapper iriMapperCore = new SimpleIRIMapper(
				IRI.create("http://www.ibm.com/SCTC/ontology/CoreSpatioTemporalDataSensorOntology.owl"),
				IRI.create(new File("CoreSpatioTemporalDataSensorOntology.owl")));
		manager.addIRIMapper(iriMapperCore);
		SimpleIRIMapper iriMapperTravelTime = new SimpleIRIMapper(
				IRI.create("http://www.ibm.com/SCTC/ontology/TravelTimeOntology.owl"), 
				IRI.create(new File("TravelTimeOntology.owl")));
		manager.addIRIMapper(iriMapperTravelTime);
*/		
		
		OWLOntologyMerger merger = new OWLOntologyMerger(manager);
//		OWLOntology timeOntology = 
//			manager.loadOntologyFromOntologyDocument(
//					IRI.create(new File("Time.owl")));
		manager.loadOntologyFromOntologyDocument(
					IRI.create(new File(
							"CoreSpatioTemporalDataSensorOntology.owl")));
		manager.loadOntologyFromOntologyDocument(
					IRI.create(new File("TravelTimeOntology.owl")));
					
//		OWLOntology ontology = manager.loadOntologyFromOntologyDocument(
//				IRI.create(new File(args[0])));
		String s = "merged-spatio-temporal-travel.owl";
//		manager.removeOntology(timeOntology);
		OWLOntology mergedOntology = 
			merger.createMergedOntology(manager, IRI.create(new File(s)));
		System.out.println("Normalizing...");
		Normalizer normalizer = new Normalizer(manager, mergedOntology);
		OWLOntology mergedNormalizedOntology = normalizer.Normalize();
		manager.saveOntology(mergedNormalizedOntology, 
				new OWLXMLOntologyFormat(), IRI.create(
						new File("norm-" + s)));
	}
	
	private static void testConnection(String[] args) {
		int port = Integer.parseInt(args[1]);
		Jedis jedis = null;
		try {
			jedis = new Jedis(args[0], port);
			System.out.println(jedis.ping());
		}
		finally {
			if(jedis != null)
				jedis.disconnect();
		}
	}
	
	public static void testNamedIndividual() throws Exception {
		OWLOntologyManager manager = OWLManager.createOWLOntologyManager();
        File owlFile = new File("trips-20130507-0.nt");
        IRI documentIRI = IRI.create(owlFile);
        OWLOntology ontology = 
        	manager.loadOntologyFromOntologyDocument(documentIRI);
        System.out.println("Ontology loaded from document");
        
	}
	
	public static void testLoadPropFile() throws Exception {
		Properties p = new Properties();
		p.load(new FileInputStream("ShardInfo.properties"));
		if(p != null) {
			Set<Entry<Object, Object>> entries  = p.entrySet();
			for(Entry<Object, Object> entry : entries)
				System.out.println(entry.getKey() + "  " + entry.getValue());
		}
	}
		
	public static void testRoleChainLoad1() {
		Jedis jedis = new Jedis("nimbus10", 6479, Constants.INFINITE_TIMEOUT);
		jedis.select(1);
		Set<String> keys = jedis.keys("*");
		System.out.println("No of keys: " + keys.size());
		long maxSize = 0;
		for(String k : keys) {
			if(jedis.type(k).equals("zset")) {
				Long size = jedis.zcard(k);
				System.out.println("Key: " + k + "  Size: " + size);
				if(size > maxSize)
					maxSize = size;
			}
		}
		System.out.println("\nMax Size: " + maxSize);
		jedis.disconnect();
	}
	
	public static void testRoleChainLoad2() {
		Jedis jedis = new Jedis("nimbus10", 6479, Constants.INFINITE_TIMEOUT);
		jedis.select(4);
		Set<String> keys = jedis.keys("*");
		System.out.println("No of keys: " + keys.size());
		long maxSize = 0;
		for(String k : keys) {
			if(jedis.type(k).equals("set")) {
				Long size = jedis.scard(k);
				System.out.println("Key: " + k + "  Size: " + size);
				if(size > maxSize)
					maxSize = size;
			}
		}
		System.out.println("\nMax Size: " + maxSize);
		jedis.disconnect();
	}
	
	public static void checkTransactions() {
		Jedis jedis = new Jedis("nimbus4", 6479);
		for(int i=1; i<=20; i++)
			jedis.sadd("trans", "t"+i);
		Transaction t = jedis.multi();
		Response<Set<String>> response = t.smembers("trans");
		t.del("trans");
		t.exec();
		System.out.println("Response size: " + response.get().size());
		for(String s : response.get())
			System.out.println(s);
		jedis.disconnect();
	}
	
	public static void initAxiomTypes() {
		Jedis jedis = new Jedis("nimbus2", 6479);
		jedis.set("type", "CR_TYPE1");
		jedis.disconnect();
		
		jedis = new Jedis("nimbus3", 6479);
		jedis.set("type", "CR_TYPE2");
		jedis.disconnect();
		
		jedis = new Jedis("nimbus4", 6479);
		jedis.set("type", "CR_TYPE3_1");
		jedis.disconnect();
		
		jedis = new Jedis("nimbus5", 6479);
		jedis.set("type", "CR_TYPE3_2");
		jedis.disconnect();
		
		jedis = new Jedis("nimbus6", 6479);
		jedis.set("type", "CR_TYPE4");
		jedis.disconnect();
	}
	
	private static void convertToFunctionalSyntax(String ontoFile) throws Exception {
		OWLOntologyManager manager = OWLManager.createOWLOntologyManager();
        File owlFile = new File(ontoFile);
        IRI documentIRI = IRI.create(owlFile);
        OWLOntology ontology = manager.loadOntologyFromOntologyDocument(documentIRI);
        
        manager.saveOntology(ontology, new OWLFunctionalSyntaxOntologyFormat(), 
        		IRI.create(new File("func-syntax.owl")));
        System.out.println("Done converting into another format");
	}
	
	private static void testMultiZRangeByScore() {
		Jedis jedis = new Jedis("nimbus5", 6479);
		jedis.select(1);
		
		Transaction roleTransaction = jedis.multi();
		Response<Set<Tuple>> keySetResponse2 = roleTransaction.zrangeByScoreWithScores(
												"localkeys", 0, Double.POSITIVE_INFINITY);
		Response<Set<String>> totalKeys2 = roleTransaction.zrange("localkeys", 0, -1);
		roleTransaction.exec();
		for(Tuple t : keySetResponse2.get()) {
			System.out.println("Element: " + t.getElement() + "  Score: " + t.getScore());
		}
		System.out.println("\nTotal keys: " + totalKeys2.get().size());
		jedis.disconnect();
	}
	
	private static void printOntologyStats(String ontPath) throws Exception {
		OWLOntologyManager manager = OWLManager.createOWLOntologyManager();
        File owlFile = new File(ontPath);
        IRI documentIRI = IRI.create(owlFile);
        OWLOntology ontology = manager.loadOntologyFromOntologyDocument(documentIRI);
        
        System.out.println("No of logical axioms: " + ontology.getLogicalAxiomCount());
        System.out.println("No of classes: " + ontology.getClassesInSignature().size());
        System.out.println("No of data properties: " + 
        		ontology.getDataPropertiesInSignature().size());
        System.out.println("No of object properties: " + 
        		ontology.getObjectPropertiesInSignature().size());
/*        
        System.out.println("No of ABox axioms: " + ontology.getABoxAxioms(false).size());
        System.out.println("No of TBox axioms: " + ontology.getTBoxAxioms(false).size());
        System.out.println("No of RBox axioms: " + ontology.getRBoxAxioms(false).size());
*/        
	}
		
	private static void testLuaTable() {
		Jedis jedis = new Jedis("localhost", 6379);
		jedis.connect();
		jedis.flushDB();
		
		String script = "t = {} " +
						"score = 2 " +
						"t[1] = tostring(score) " +
						"t[2] = tostring(1) " +
						"return t ";
		String script1 = "t = {} t[1] = '1' t[2] = '2' return t";
		String script2 = 
		  "size = redis.call('ZCARD', 'localkeys') " +
			  "if(size == 0) then " +
			  		"score = 1.0 " +
			  "else " +
			  		"escore = redis.call('ZRANGE', 'localkeys', -1, -1, 'WITHSCORES') " +
			  		"score = escore[2] + " + Constants.SCORE_INCREMENT + " " +
			  "end " +
			  "return score ";
		jedis.zadd("localkeys", 4.0, "abc");
		Object obj = jedis.eval(script2, 
									Collections.singletonList("abc"), 
									Collections.singletonList("100"));
		System.out.println(obj.getClass().getCanonicalName());
		System.out.println(((Long)obj).longValue());
		
		
		jedis.flushDB();
		jedis.disconnect();
	}
	
	
	private static void compareReasoners(String ontPath) throws Exception {
		// comparing Pellet & ELK
		OWLOntologyManager manager = OWLManager.createOWLOntologyManager();
        File owlFile = new File(ontPath);
        IRI documentIRI = IRI.create(owlFile);
        OWLOntology ontology = manager.loadOntologyFromOntologyDocument(documentIRI);
        
        GregorianCalendar r1Start = new GregorianCalendar();
        // ELK
        System.out.println("Precomputing for ELK");
        OWLReasonerFactory reasonerFactory = new ElkReasonerFactory();
        OWLReasoner reasonerELK = reasonerFactory.createReasoner(ontology);
        reasonerELK.precomputeInferences(InferenceType.CLASS_HIERARCHY);
        
        GregorianCalendar r1End = new GregorianCalendar();
        
        System.out.println("Precomputing for Pellet");
        PelletReasoner pelletReasoner = PelletReasonerFactory.getInstance().createReasoner( ontology );
	    pelletReasoner.prepareReasoner();
	    
	    GregorianCalendar r2End = new GregorianCalendar();
	    double diff1 = (r1End.getTimeInMillis() - r1Start.getTimeInMillis())/1000;
		long completionTimeMin1 = (long)diff1/60;
		double completionTimeSec1 = diff1 - (completionTimeMin1 * 60);
		
		System.out.println("ELK precomputing time: " + 
				completionTimeMin1 + " mins and " + completionTimeSec1 + " secs");
		
		double diff2 = (r2End.getTimeInMillis() - r1End.getTimeInMillis())/1000;
		long completionTimeMin2 = (long)diff2/60;
		double completionTimeSec2 = diff2 - (completionTimeMin2 * 60);
		
		System.out.println("Pellet precomputing time: " + 
				completionTimeMin2 + " mins and " + completionTimeSec2 + " secs");
		
		Set<OWLClass> concepts = ontology.getClassesInSignature();
		for(OWLClass cl : concepts) {
			Set<OWLClass> superClasses1 = reasonerELK.getSuperClasses(cl, false).getFlattened();
			Set<OWLClass> superClasses2 = pelletReasoner.getSuperClasses(cl, false).getFlattened();
			
			Iterator<OWLClass> iterator = reasonerELK.getEquivalentClasses(cl).iterator();
			while(iterator.hasNext()) {
				OWLClass ocl = iterator.next();
				superClasses1.add(ocl);
				superClasses2.add(ocl);
			}
			if(superClasses1.size() != superClasses2.size()) {
				System.out.println("\nNot equal for " + cl.toString());
				superClasses1.removeAll(superClasses2);
				System.out.println("Remaining classes");
				for(OWLClass ecl : superClasses1)
					System.out.println(ecl.toString());
			}			
		}
		
		reasonerELK.dispose();
		pelletReasoner.dispose();
	}
	
	private static void testZRangeByScore() {
		Jedis jedis = new Jedis("localhost", 6379);
		jedis.connect();
		jedis.flushDB();
		
		String key = "1359~4339";
		jedis.zadd(key, 1.0, "1360");
		jedis.zadd(key, 1.0016999999999998, "2283");
		jedis.zadd(key, 1.0024999999999997, "2311");
		jedis.zadd(key, 1.0024999999999997, "4193");
		
		Set<Tuple> tuples = jedis.zrangeByScoreWithScores(key, 1.0017999999999998, 
										Double.POSITIVE_INFINITY);
		for(Tuple t : tuples)
			System.out.println(t.getElement() + "    " + t.getScore());
		
		jedis.flushDB();
		jedis.disconnect();
	}
	
	private static void suppressNewScoreZAdd() {
		Jedis jedis = new Jedis("localhost", 6379);
		jedis.connect();
		jedis.flushDB();
		
		String checkAndInsertScript = "size = #KEYS " +
		  							  "for i=1,size do " +
		  							  	"if(not redis.call('ZSCORE', KEYS[i], ARGV[i])) then " +
		  							  		"if(redis.call('ZCARD', KEYS[i]) == 0) then " +
		  							  			"redis.call('ZADD', KEYS[i], 1.0, ARGV[i]) " +
		  							  		"else " +
		  							  			"escore = redis.call('ZRANGE', KEYS[i], -1, -1, 'WITHSCORES') " +
		  							  			"score = escore[2] + 0.0001 " +
		  							  			"redis.call('ZADD', KEYS[i], score, ARGV[i]) " +
		  							  		"end " +
		  							  	"end " +
		  							  "end ";

		
		List<String> keyList = new ArrayList<String>();
		String key1 = "foo1";
		jedis.zadd(key1, 0.5, "bar1");
		jedis.zadd(key1, 0.5, "bar2");
		jedis.zadd(key1, 0.5, "bar3");
		
		String key2 = "foo2";
		jedis.zadd(key2, 1.5, "bar10");
		jedis.zadd(key2, 2.5, "bar20");
		jedis.zadd(key2, 3.5, "bar30");
		
		String key3 = "foo3";
		jedis.zadd(key3, 4.5, "bar1");
		jedis.zadd(key3, 3.5, "bar10");
		jedis.zadd(key3, 2.5, "bar31");
	
		keyList.add(key1);
		keyList.add(key1);
		keyList.add(key2);
		keyList.add(key2);
		keyList.add(key3);
		keyList.add(key3);
		
		List<String> vals = new ArrayList<String>();
		vals.add("bar1");
		vals.add("bar11");
		vals.add("bar20");
		vals.add("bar15");
		vals.add("bar10");
		vals.add("bar101");
		
		jedis.eval(checkAndInsertScript, keyList, vals);

		for(int i=1; i<=3; i++) {
			String key = "foo" + i;
			Set<Tuple> elements = jedis.zrangeWithScores(key, 0, -1);
			System.out.println("for key: " + key);
			for(Tuple s : elements)
				System.out.println(s.getElement() + "   " + s.getScore());
			System.out.println();
		}
		
		jedis.flushDB();
		jedis.disconnect();
	}
	
	private static void printAxioms(String ontPath) throws Exception {
		OWLOntologyManager manager = OWLManager.createOWLOntologyManager();
        File owlFile = new File(ontPath);
        IRI documentIRI = IRI.create(owlFile);
        OWLOntology ontology = manager.loadOntologyFromOntologyDocument(documentIRI);
        
        String testClassYStr = "<http://www.co-ode.org/ontologies/galen#Arthroscope>";
		String testRoleStr = "<http://www.co-ode.org/ontologies/galen#hasPhysicalMeans>";
		String testClassXStr = "<http://knoelab.wright.edu/ontologies#Class1110>";
		
		Set<OWLClass> cls = ontology.getClassesInSignature();
		OWLClass testClassY = null;
		for(OWLClass cl : cls)
			if(cl.toString().equals(testClassYStr)) {
				testClassY = cl;
				System.out.println("Found " + testClassYStr);
			}
		Set<OWLObjectProperty> dps = ontology.getObjectPropertiesInSignature();
		OWLObjectProperty testRole = null;
		for(OWLObjectProperty dp : dps)
			if(dp.toString().equals(testRoleStr)) {
				testRole = dp;
				System.out.println("Found " + testRoleStr);
			}
 		
		Set<OWLAxiom> propAxioms = ontology.getReferencingAxioms(testRole);
        
        for(OWLAxiom axiom : propAxioms){
        	System.out.println(axiom.toString());
        }
	}
	
	private static void testLua() {
		Jedis jedis = new Jedis("localhost", 6379);
		jedis.connect();
		jedis.flushDB();
		
		String key1 = "foo1";
		jedis.zadd(key1, 1.0, "a1");
		jedis.zadd(key1, 2.0, "a2");
		jedis.zadd(key1, 3.0, "a3");
/*				
		String key2 = "foo2";
		jedis.zadd(key2, 1.0001, "b1");
		jedis.zadd(key2, 1.0002, "a2");
		jedis.zadd(key2, 1.0003, "a3");
		
		String key3 = "foo3";
		jedis.zadd(key3, 1.0001, "c1");
		jedis.zadd(key3, 1.0002, "a2");
		jedis.zadd(key3, 1.0003, "a3");
*/		
		String val1 = "bar1";
		String val2 = "bar2";
		String val3 = "bar3";
		
		jedis.zadd(val1, 1.0, val1);
		jedis.zadd(val2, 2.0, val2);
		jedis.zadd(val3, 3.0, val3);
		
		
		String scriptSingleConcept = "elementScore = redis.call('ZRANGE', KEYS[1], -1, -1, 'WITHSCORES') " +
		  "unique = 0 " +
		  "t= {} " +
		  "minScore = table.remove(ARGV) " +
		  "toBeAddedList = redis.call('ZRANGEBYSCORE', KEYS[1], minScore, '+inf') " +
		  "for index1,value1 in pairs(ARGV) do " +
		  	"escore = redis.call('ZRANGE', value1, -1, -1, 'WITHSCORES') " +
		  	"score = escore[2] + " + Constants.SCORE_INCREMENT + " " +
		  	"for index2,value2 in pairs(toBeAddedList) do " +
		  		"if(not redis.call('ZSCORE', value1, value2)) then " +
		  			"ret = redis.call('ZADD', value1, score, value2) " +
		  			"unique = unique + ret " +
		  		"end " +
		  	"end " +
		  "end " +
		  "t[1] = tostring(elementScore[2]) " +
		  "t[2] = tostring(unique) " +	
		  "return t ";
		
		String script = "if (redis.call('ZSCORE', 'foo1', 'a5') == nil) then " +
							"return 1 " +
						"else " +
							"return 2 " +
						"end ";
		
		List<String> args = new ArrayList<String>();
		args.add(val1);
		args.add(val2);
		args.add(val3);
		
		List<String> keys = new ArrayList<String>();
		keys.add(key1);
//		keys.add(key2);
//		keys.add(key3);
		
		args.add("(" + 1.0);
//		String nextMinScoreStr =  (String) jedis.eval(scriptSingleConcept, keys, argsArray);
		
		ArrayList<String> table = (ArrayList<String>) jedis.eval(scriptSingleConcept, 
										Collections.singletonList(key1), args);
		for(String s : table)
			System.out.println(s);
		
//		jedis.flushDB();
		jedis.disconnect();
	}
	
	private static void testRedisScripting() {
		Jedis jedis = new Jedis("localhost", 6379);
		jedis.connect();
		jedis.flushDB();
		
		byte[] key = SafeEncoder.encode("foo");
		jedis.zadd(key, 1.0001, SafeEncoder.encode("bar1"));
		jedis.zadd(key, 4.0001, SafeEncoder.encode("bar2"));
		jedis.zadd(key, 4.0003, SafeEncoder.encode("bar3"));
		
		// call zrange from within script and check it out.
		Set<Tuple> elementScores = jedis.zrangeWithScores(key, -1, -1);
		
		for(Tuple t : elementScores) 
			System.out.println(SafeEncoder.encode(t.getBinaryElement()) + "  " + t.getScore());
		System.out.println();
		
		
		String script = "x = redis.call('zrange',KEYS[1],'-1','-1','withscores') " +
				"score = x[2] + 0.0001 redis.call('zadd',KEYS[1],score,ARGV[1])";
		
		Object response = jedis.eval(SafeEncoder.encode(script), 
				Collections.singletonList(key), 
				Collections.singletonList(SafeEncoder.encode("bar4")));
		
//		System.out.println(response.getClass().getName());
		
		System.out.println("With script...");
		elementScores = jedis.zrangeWithScores(key, -1, -1);
		for(Tuple t : elementScores) 
			System.out.println(SafeEncoder.encode(t.getBinaryElement()) + "  " + t.getScore());
        
		jedis.flushDB();
        jedis.disconnect();
	}
	
	private static void testSMembers() throws Exception {
		Jedis jedis = new Jedis("localhost", 6379);
		byte[] k = ByteBuffer.allocate(Constants.NUM_BYTES).putInt(2).array();
		byte[] v = jedis.get(k);
		if(v == null)
			System.out.println("Val is null");
		else
			System.out.println("Val is not null");
		byte[] key1 = new String("A").getBytes("UTF-8");
		for(int i=1; i<=3; i++)
			jedis.sadd(key1, new String("A" + i).getBytes("UTF-8"));
		
		byte[] key2 = new String("B").getBytes("UTF-8");
		for(int i=1; i<=3; i++)
			jedis.sadd(key2, new String("B" + i).getBytes("UTF-8"));
		
		byte[] key3 = new String("C").getBytes("UTF-8");
		for(int i=1; i<=3; i++)
			jedis.sadd(key3, new String("C" + i).getBytes("UTF-8"));
		
		Pipeline p = jedis.pipelined();
		List<Response<Set<byte[]>>> response = new ArrayList<Response<Set<byte[]>>>();
/*		
		response.add(p.smembers(key2));
		response.add(p.smembers(key1));
		response.add(p.smembers(key3));
*/		
		p.sync();
		
		for(Response<Set<byte[]>> r : response) {
			for(byte[] data : r.get())
				System.out.println(new String(data, "UTF-8"));
			System.out.println();
		}
		
		jedis.flushDB();
		jedis.disconnect();
	}
	
	// without pipeline - 16 secs, with pipeline - 2 secs
	private static void testPipelineReadType3_1() throws Exception {
		Jedis jedis = new Jedis("localhost", 6379);
		Jedis resultStore = new Jedis("nimbus.cs.wright.edu", 6379);
		final int QUEUE_SIZE = 5000;
		try {
			PropertyFileHandler propertyFileHandler = PropertyFileHandler.getInstance();
			byte[] localKeys = propertyFileHandler.getLocalKeys().
								getBytes(propertyFileHandler.getCharset());
			Set<byte[]> keys = jedis.smembers(localKeys);
			System.out.println("No of keys: " + keys.size());
			GregorianCalendar startTime = new GregorianCalendar();
			for(byte[] k : keys)
				resultStore.smembers(k);
			System.out.print("Localhost -- Without pipelining...");
			System.out.println("Time taken (millis): " + 
					Util.getElapsedTime(startTime));
			System.out.println("Localhost -- With pipelining...");
			GregorianCalendar startTime1 = new GregorianCalendar();
			Pipeline p = resultStore.pipelined();
			int index = 1;
			List<Response<Set<byte[]>>> response = new ArrayList<Response<Set<byte[]>>>();
			for(byte[] k : keys) {
//				response.add(p.smembers(k));
				if((index%QUEUE_SIZE) == 0)
					p.sync();
			}
			if((index%QUEUE_SIZE) != 0)
				p.sync();
			// now iterate through the list
			for(Response<Set<byte[]>> sr : response) {
				
			}
			System.out.println("Time taken (millis): " + 
					Util.getElapsedTime(startTime1));		
		}
		finally {
			jedis.disconnect();
			resultStore.disconnect();
		}
	}
	
	// without pipeline - 47 secs, with pipeline - 16 secs
	private static void testPipelineReadType3_2() throws Exception {
		Jedis jedis = new Jedis("localhost", 6379);
		final int QUEUE_SIZE = 5000;
		try {
			PropertyFileHandler propertyFileHandler = PropertyFileHandler.getInstance();
			byte[] localKeys = propertyFileHandler.getLocalKeys().
								getBytes(propertyFileHandler.getCharset());
			Set<byte[]> keys = jedis.smembers(localKeys);
			System.out.println("No of keys: " + keys.size());
			GregorianCalendar startTime = new GregorianCalendar();
			for(byte[] k : keys)
				jedis.smembers(k);
			System.out.print("Localhost -- Without pipelining...");
			System.out.println("Time taken (millis): " + 
					Util.getElapsedTime(startTime));
			System.out.println("Localhost -- With pipelining...");
			GregorianCalendar startTime1 = new GregorianCalendar();
			Pipeline p = jedis.pipelined();
			int index = 1;
			List<Response<Set<byte[]>>> response = new ArrayList<Response<Set<byte[]>>>();
			for(byte[] k : keys) {
//				response.add(p.smembers(k));
				if((index%QUEUE_SIZE) == 0)
					p.sync();
			}
			if((index%QUEUE_SIZE) != 0)
				p.sync();
			// now iterate through the list
			for(Response<Set<byte[]>> sr : response) {
				
			}
			System.out.println("Time taken (millis): " + 
					Util.getElapsedTime(startTime1));		
		}
		finally {
			jedis.disconnect();
		}
	}
	
	private static void testSortedSetMethods() throws Exception {
		Jedis jedis = new Jedis("localhost", 6379);
		try {
			jedis.flushDB();
			byte[] key = "A".getBytes("UTF-8");
			double score = 1;
			for(int i=1; i<=3; i++) {
				byte[] member = ByteBuffer.allocate(Constants.NUM_BYTES).putInt(i).array();
				jedis.zadd(key, score, member);
			}
			score = 6;
			for(int i=3; i<=6; i++) {
				byte[] member = ByteBuffer.allocate(Constants.NUM_BYTES).putInt(i).array();
				jedis.zadd(key, score, member);
			}
			score = 8;
			for(int i=6; i<=9; i++) {
				byte[] member = ByteBuffer.allocate(Constants.NUM_BYTES).putInt(i).array();
				jedis.zadd(key, score, member);
			}
			System.out.println("No of members in the set: " + jedis.zcard(key));
			Set<Tuple> valScores = jedis.zrangeByScoreWithScores(key, Double.NEGATIVE_INFINITY, 
									Double.POSITIVE_INFINITY);
			byte[] posInf = "+inf".getBytes(Protocol.CHARSET);
			String exclScore = "(" + 2;
			byte[] exclScoreByte = exclScore.getBytes(Protocol.CHARSET);
			Set<Tuple> exclSet = jedis.zrangeByScoreWithScores(key, exclScoreByte, posInf);
			for(Tuple tuple : exclSet) {
				byte[] val = tuple.getBinaryElement();
				int valInt = ByteBuffer.wrap(val).getInt();
				System.out.println("Val: " + valInt + "  Score: " + tuple.getScore());
			}
		}
		finally {
			jedis.flushDB();
			jedis.disconnect();
		}
	}
	
	private static void testNullVals() {
		Jedis jedis = new Jedis("localhost", 6379);
		try {
			jedis.flushAll();
			byte[] b = ByteBuffer.allocate(4).putInt(2).array();
			byte[] c = ByteBuffer.allocate(4).putInt(4).array();
			jedis.sadd(b, c);
			byte[] e = jedis.spop(b);
			if(e == null)
				System.out.println("e is null");
			else
				System.out.println("e is not null");
			long keyCount1 = jedis.scard(b);
			byte[] k = jedis.spop(b);
			long keyCount2 = jedis.scard(b);
			if(k == null)
				System.out.println("It is null");
			else
				System.out.println("It is not null");
			System.out.println("Key count1: " + keyCount1 + " key count2: " + keyCount2);
			jedis.smembers(k);
		}
		finally {
			jedis.disconnect();
		}
	}
	
	private static void printKeyCount() throws Exception {
		byte[] localKeys = PropertyFileHandler.getInstance().getLocalKeys().getBytes("UTF-8");
		Jedis jedis = new Jedis("localhost", 6379);
		System.out.println("Key count: " + jedis.scard(localKeys));
		jedis.disconnect();
	}
	
	private static void getMaxSuperClassCount() throws Exception {
		Jedis jedis = new Jedis("localhost", 6379);
		jedis.select(1);
		
		// delete "type" key
		byte[] crTypeKey = new String("type").getBytes("UTF-8");
		jedis.del(crTypeKey);
		
		byte[] allKeysPattern = new String("*").getBytes("UTF-8");
		Set<byte[]> allKeys = jedis.keys(allKeysPattern);
		long count = -1;
		
		for(byte[] key : allKeys) {
			long keyCount = jedis.scard(key);
			if(keyCount > count)
				count = keyCount;
		}
		System.out.println("No. of keys: " + allKeys.size());
		System.out.println("Max. key count: " + count);
	}
	
	private static void testSetContainment() {
		Set<byte[]> concepts = new HashSet<byte[]>();
		ByteBuffer one = ByteBuffer.allocate(Constants.NUM_BYTES).putInt(1);
		ByteBuffer two = ByteBuffer.allocate(Constants.NUM_BYTES).putInt(2);
		
		concepts.add(one.array());
		concepts.add(two.array());
		
		ByteBuffer dummyOne = ByteBuffer.allocate(Constants.NUM_BYTES).putInt(1);
		System.out.println(concepts.contains(dummyOne.array()));
		System.out.println(one.equals(dummyOne));
		
		Jedis jedis = new Jedis("localhost", 6379);
		jedis.sadd(one.array(), dummyOne.array());
		jedis.sadd(one.array(), two.array());
		
		System.out.println("From Jedis: " + jedis.sismember(one.array(), two.array()));
		jedis.flushAll();
		jedis.disconnect();
	}
	
	private static void testUnionStoreAlt() {
		//TODO: next check with zsets
		
		Jedis jedis = new Jedis("localhost", 6479, Constants.INFINITE_TIMEOUT);
		try {
			/* Tests the performance of alternatives to sunionstore
			 * 1) sunionstore
			 * 2) script
			 * 3) smembers + sadd (pipeline)
			 */
			
			fillData(jedis);
			GregorianCalendar start = new GregorianCalendar();
			jedis.zunionstore("k2", "k1", "k2");
			System.out.println("For zunionstore...");
			System.out.println("Time taken (millis): " + 
					Util.getElapsedTime(start));
			System.out.println();
			jedis.del("k2");
			
			fillData(jedis);
			start = new GregorianCalendar();
			String script = 
				"elements = redis.call('ZRANGE', 'k1', 0, -1) " +
				"size = #elements " +
				"for i=1,size do " +
					"redis.call('ZADD', 'k2', 1.0, elements[i]) " +
				"end ";
			jedis.eval(script);
			System.out.println("For script...");
			System.out.println("Time taken (millis): " + 
					Util.getElapsedTime(start));
			System.out.println();
			
			jedis.del("k2");
			jedis.flushAll();
			fillData(jedis);
			 start = new GregorianCalendar();
			Set<Tuple> members = jedis.zrangeWithScores("k1", 0, -1);
			System.out.println("members: " + members.size());
			HostInfo hinfo = new HostInfo("localhost",6479);
			PipelineManager pipelineManager = 
				new PipelineManager(Collections.singletonList(hinfo), 10000);
			for(Tuple s : members)
				pipelineManager.pzadd(hinfo, "k2", s.getScore(), 
						s.getElement(), AxiomDB.NON_ROLE_DB);
			pipelineManager.synchAll(AxiomDB.NON_ROLE_DB);
			members.clear();
			pipelineManager.resetSynchResponse();
			pipelineManager.closeAll();
			System.out.println("For pipeline...");
			System.out.println("Time taken (millis): " + 
					Util.getElapsedTime(start));
			System.out.println();		
		}
		finally {
			jedis.disconnect();
		}
	}
	
	private static void fillData(Jedis jedis) {
		GregorianCalendar start = new GregorianCalendar();
		jedis.flushAll();
		HostInfo hinfo = new HostInfo("localhost",6479);
		PipelineManager pipelineManager = 
			new PipelineManager(Collections.singletonList(hinfo), 10000);
//		Random r = new Random();
		double score = 1.0;
		Long l = jedis.zcard("k1");
		System.out.println("Before insertion: " + l);
		for(int i=1; i<=3000000; i++) {
			pipelineManager.pzadd(hinfo, "k1", score, Long.toString(i), 
					AxiomDB.NON_ROLE_DB);
			if(i%1000 == 0)
				score++;
		}
		pipelineManager.synchAll(AxiomDB.NON_ROLE_DB);
		pipelineManager.resetSynchResponse();
		pipelineManager.closeAll();
		l = jedis.zcard("k1");
		System.out.println("After insertion: " + l);
		System.out.println("Time taken to fill 3M elements..." + 
				Util.getElapsedTime(start));
		System.out.println();
	}
	
	public static void testLuaZRange() {
		String script = 
			"escore = redis.call('ZRANGE', 'k3', -1, -1, 'WITHSCORES') " +
			"return tonumber(escore[2]) ";
		Jedis jedis = new Jedis("localhost", 6479, Constants.INFINITE_TIMEOUT);
		jedis.connect();
		Long n = (Long) jedis.eval(script);
		System.out.println(n);
		jedis.disconnect();
	}
}

class PubSubHandler extends JedisPubSub implements Runnable {

	private Jedis jedis;
	private CountDownLatch latch;
	
	PubSubHandler(Jedis jedis, CountDownLatch latch) {
		this.jedis = jedis;
		this.latch = latch;
	}
	
	@Override
	public void run() {
		jedis.subscribe(this, "progress");
		latch.countDown();
	}		
	
	@Override
	public void onUnsubscribe(String channel, int subscribedChannels) {
		System.out.println("Unsubscribed...");
	}
	
	@Override
	public void onSubscribe(String channel, int subscribedChannels) {
		System.out.println("Subscribed to " + channel 
				+ "  num: " + subscribedChannels);				
	}
	
	@Override
	public void onPUnsubscribe(String arg0, int arg1) { }			
	@Override
	public void onPSubscribe(String arg0, int arg1) { }			
	@Override
	public void onPMessage(String arg0, String arg1, String arg2) {	}
	
	@Override
	public void onMessage(String channel, String message) {
		System.out.println("Received message: " + message);
	}
}


