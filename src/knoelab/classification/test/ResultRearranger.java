package knoelab.classification.test;

import java.io.File;
import java.io.PrintWriter;
import java.util.GregorianCalendar;
import java.util.Iterator;
import java.util.Set;

import knoelab.classification.init.AxiomDistributionType;
import knoelab.classification.misc.Constants;
import knoelab.classification.misc.HostInfo;
import knoelab.classification.misc.PropertyFileHandler;
import knoelab.classification.misc.Util;

import org.semanticweb.owlapi.apibinding.OWLManager;
import org.semanticweb.owlapi.model.IRI;
import org.semanticweb.owlapi.model.OWLClass;
import org.semanticweb.owlapi.model.OWLOntology;
import org.semanticweb.owlapi.model.OWLOntologyManager;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.exceptions.JedisException;

import com.clarkparsia.pellet.owlapiv3.PelletReasoner;
import com.clarkparsia.pellet.owlapiv3.PelletReasonerFactory;

public class ResultRearranger {

	public void initializeAndRearrange() throws Exception {
		
		PropertyFileHandler propertyFileHandler = PropertyFileHandler.getInstance();
	    HostInfo resultNodeHostInfo = propertyFileHandler.getResultNode();
	    Jedis resultStore0 = new Jedis(resultNodeHostInfo.getHost(), 
	    					resultNodeHostInfo.getPort(), Constants.INFINITE_TIMEOUT);
	    resultStore0.select(0);
	    Jedis resultStore1 = new Jedis(resultNodeHostInfo.getHost(), 
	    					resultNodeHostInfo.getPort(), Constants.INFINITE_TIMEOUT);
	    resultStore1.select(1);
	    // TODO: Don't hardcode the ID store
//	    Jedis idReader = new Jedis("nimbus5.cs.wright.edu", 6380, Constants.INFINITE_TIMEOUT);
	    
		try {
/*			
			OWLOntologyManager manager = OWLManager.createOWLOntologyManager();
			
			System.out.println("Comparing classification output for " + ontPath);
			File ontFile = new File(ontPath);
			IRI documentIRI = IRI.create(ontFile);
			OWLOntology ontology = manager.loadOntologyFromOntologyDocument(documentIRI);
		    System.out.println("Not Normalizing");
		
		    GregorianCalendar cal1 = new GregorianCalendar();
		    PelletReasoner pelletReasoner = PelletReasonerFactory.getInstance().createReasoner( ontology );
		    pelletReasoner.prepareReasoner();
		    GregorianCalendar cal2 = new GregorianCalendar();
			double diff = (cal2.getTimeInMillis() - cal1.getTimeInMillis())/1000;
			long completionTimeMin = (long)diff/60;
			double completionTimeSec = diff - (completionTimeMin * 60);
			
			System.out.println("Pellet completed in " + completionTimeMin + " mins and " + completionTimeSec + " secs");
*/			
			System.out.println("Rearrranging results...");
			rearrangeClassifierResults(resultStore0, resultStore1);
//			System.out.println("Printing results....");
//			printResults(ontology, pelletReasoner, resultStore, idReader);
			System.out.println("Done");
		}
		finally {
			resultStore0.disconnect();
			resultStore1.disconnect();
//			idReader.disconnect();
		}
	}
	
	private void rearrangeClassifierResults(Jedis resultStore0, Jedis resultStore1) throws Exception {
		
		resultStore1.flushDB();
		
		// move "type" and "TypeHost" key to DB1
		resultStore0.move(Constants.KEYS_UPDATED, 1);
		for(AxiomDistributionType type : AxiomDistributionType.values())
			resultStore0.move(type.toString(), 1);
		resultStore0.move("channel", 1);
		resultStore0.move("ChannelSet", 1);
		resultStore0.move("type", 1);
		
		Pipeline p = resultStore1.pipelined();
		Set<String> allKeys = resultStore0.keys("*");		
		double keyCount = 0;
		int multiplier = 1;
		
		for(String key : allKeys) {
			try{
				Set<String> values = resultStore0.zrange(key, 0, -1);
				for(String xchangeKey : values)
					p.sadd(xchangeKey, key);
			}
			catch(JedisException e) {
				System.out.println("Invalid key: " + key);
				throw e;
			}
			
			keyCount++;
			double keyProgress = (keyCount/allKeys.size())*100;
			if(keyProgress >= (5*multiplier)) {
				System.out.println("% of no. of keys rearranged: " + keyProgress);
				multiplier++;
				p.sync();
			}
		}
		p.sync();
		// move the 2 type keys back to DB0
		for(AxiomDistributionType type : AxiomDistributionType.values())
			resultStore1.move(type.toString(), 0);
		resultStore1.move("ChannelSet", 0);
		resultStore1.move("channel", 0);
		resultStore1.move("type", 0);
		resultStore1.move(Constants.KEYS_UPDATED, 0);
	}
	
	private void printResults(OWLOntology ontology, 
			PelletReasoner reasoner, Jedis resultStore, Jedis idReader) throws Exception {
	    
		resultStore.select(1);
		PrintWriter writer1 = new PrintWriter("Pellet-output.txt", "UTF-8");
		PrintWriter writer2 = new PrintWriter("MyOutput.txt", "UTF-8");
		try {
		    Set<OWLClass> classes = ontology.getClassesInSignature();
		    for(OWLClass cl : classes) {
		    	Set<OWLClass> reasonerSuperclasses = reasoner.getSuperClasses(cl, false).getFlattened();
				// add cl itself to S(X) computed by reasoner. That is missing
				// in its result.
				reasonerSuperclasses.add(cl);
				// adding equivalent classes -- they are not considered if asked for superclasses
				Iterator<OWLClass> iterator = reasoner.getEquivalentClasses(cl).iterator();
				while(iterator.hasNext())
					reasonerSuperclasses.add(iterator.next());
				
				writer1.print(cl.toString() + " = " + reasonerSuperclasses.size() + ", ");
				for(OWLClass scl : reasonerSuperclasses)
					writer1.print(scl.toString() + ", ");
				writer1.println("\n");
				
				String classToCheckID = Util.conceptToID(cl.toString(), idReader);
				Set<String> classifierResults = resultStore.smembers(classToCheckID);
				writer2.print(cl.toString() + " = " + classifierResults.size() + ", ");
				for(String clResult : classifierResults)
					writer2.print(Util.idToConcept(clResult, idReader) + ", ");
				writer2.println("\n");
		    }
		}
		finally {
			writer1.close();
			writer2.close();
		}
	}
	
	public static void main(String[] args) throws Exception {
/*		
		if(args.length != 1 || args[0].isEmpty()) {
			System.out.println("Give the path of owl file");
    		System.exit(-1);
		}
*/		
		new ResultRearranger().initializeAndRearrange();
	}

}
