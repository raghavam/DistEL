package knoelab.classification.test;

import java.io.File;
import java.io.PrintWriter;
import java.util.GregorianCalendar;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import knoelab.classification.init.AxiomDistributionType;
import knoelab.classification.misc.Constants;
import knoelab.classification.misc.HostInfo;
import knoelab.classification.misc.PropertyFileHandler;
import knoelab.classification.misc.Util;

//import org.semanticweb.HermiT.Reasoner;
import org.semanticweb.elk.owlapi.ElkReasonerFactory;
import org.semanticweb.owlapi.apibinding.OWLManager;
import org.semanticweb.owlapi.model.IRI;
import org.semanticweb.owlapi.model.OWLClass;
import org.semanticweb.owlapi.model.OWLOntology;
import org.semanticweb.owlapi.model.OWLOntologyManager;
import org.semanticweb.owlapi.reasoner.InferenceType;
import org.semanticweb.owlapi.reasoner.OWLReasoner;
import org.semanticweb.owlapi.reasoner.OWLReasonerFactory;

import redis.clients.jedis.Jedis;

import com.clarkparsia.pellet.owlapiv3.PelletReasoner;
import com.clarkparsia.pellet.owlapiv3.PelletReasonerFactory;

public class ResultDiffWriter {

	private static void writeDiffResults(OWLOntology ontology, 
			OWLReasoner reasoner, Jedis resultStore, Jedis idReader) throws Exception {
		resultStore.select(1);
		PrintWriter writer = new PrintWriter("Diff-output.txt", "UTF-8");
		try {
		    Set<OWLClass> classes = ontology.getClassesInSignature();
		    int diffCount = 0;
		    double keyCount = 0;
			int multiplier = 1;
			
		    for(OWLClass cl : classes) {
		    	Set<OWLClass> reasonerSuperclasses = reasoner.getSuperClasses(cl, false).getFlattened();
				// add cl itself to S(X) computed by reasoner. That is missing
				// in its result.
				reasonerSuperclasses.add(cl);
				// adding equivalent classes -- they are not considered if asked for superclasses
				Iterator<OWLClass> iterator = reasoner.getEquivalentClasses(cl).iterator();
				while(iterator.hasNext())
					reasonerSuperclasses.add(iterator.next());
				
				String classToCheckID = Util.conceptToID(cl.toString(), idReader);
				Set<String> classifierResults = resultStore.smembers(classToCheckID);
				
				if(reasonerSuperclasses.size() != classifierResults.size()) {
					diffCount++;
					writer.print(cl.toString() + " = " + reasonerSuperclasses.size() + ", ");
					for(OWLClass scl : reasonerSuperclasses)
						writer.print(scl.toString() + ", ");
					writer.println("\n");
					
					
					writer.print(cl.toString() + " = " + classifierResults.size() + ", ");
					for(String clResult : classifierResults)
						writer.print(Util.idToConcept(clResult, idReader) + "--" + clResult + ", ");
					writer.println("\n");
					
					
					Set<String> myResults = new HashSet<String>();
					for(String clResultID : classifierResults)
						myResults.add(Util.idToConcept(clResultID, idReader));
					
					Set<String> reasonerResults = new HashSet<String>(reasonerSuperclasses.size());
					for(OWLClass rcl : reasonerSuperclasses)
						reasonerResults.add(rcl.toString());
					
					writer.println("Diff: \n" );
					
					reasonerResults.removeAll(myResults);
					for(String s : reasonerResults)
						writer.print(s + "--" + Util.conceptToID(s, idReader) + ",  ");
					writer.println("\n");
				}
				
				keyCount++;
				double keyProgress = (keyCount/classes.size())*100;
				if(keyProgress >= (5*multiplier)) {
					System.out.println("% of no. of keys checked: " + keyProgress + " \t" + diffCount);
					multiplier++;
				}
		    }
		    System.out.println("Diff count: " + diffCount);
		}
		finally {
			writer.close();
		}
	}
	
	public static void main(String[] args) throws Exception {
		if(args.length != 1 || args[0].isEmpty()) {
			System.out.println("Give the path of owl file");
    		System.exit(-1);
		}
		PropertyFileHandler propertyFileHandler = PropertyFileHandler.getInstance();
	    HostInfo resultNodeHostInfo = propertyFileHandler.getResultNode();
	    Jedis resultStore = new Jedis(resultNodeHostInfo.getHost(), 
	    				resultNodeHostInfo.getPort(), Constants.INFINITE_TIMEOUT);

	    HostInfo localHostInfo = propertyFileHandler.getLocalHostInfo();
	    Jedis localStore = new Jedis(localHostInfo.getHost(), localHostInfo.getPort());
	    Set<String> idHosts = localStore.smembers(AxiomDistributionType.CONCEPT_ID.toString());
	    String[] idReaderHost = idHosts.iterator().next().split(":");
	    
	    localStore.disconnect();
	    Jedis idReader = new Jedis(idReaderHost[0], Integer.parseInt(idReaderHost[1]), 
	    							Constants.INFINITE_TIMEOUT);
	    
	    OWLReasoner reasonerELK = null;
//	    PelletReasoner pelletReasoner = null;
//	    Reasoner hermitReasoner = null;
	    
		try {
			OWLOntologyManager manager = OWLManager.createOWLOntologyManager();
			
			System.out.println("Comparing classification output for " + args[0]);
			File ontFile = new File(args[0]);
			IRI documentIRI = IRI.create(ontFile);
			OWLOntology ontology = manager.loadOntologyFromOntologyDocument(documentIRI);
		
		    GregorianCalendar cal1 = new GregorianCalendar();
		    OWLReasonerFactory reasonerFactory = new ElkReasonerFactory();
		    reasonerELK = reasonerFactory.createReasoner(ontology);
		    reasonerELK.precomputeInferences(InferenceType.CLASS_HIERARCHY);
//		    pelletReasoner = PelletReasonerFactory.getInstance().createReasoner( ontology );
//		    pelletReasoner.prepareReasoner();		    
//		    hermitReasoner = new Reasoner(ontology);
//		    hermitReasoner.precomputeInferences(InferenceType.CLASS_HIERARCHY);
		    
		    GregorianCalendar cal2 = new GregorianCalendar();
			double diff = (cal2.getTimeInMillis() - cal1.getTimeInMillis())/1000;
			long completionTimeMin = (long)diff/60;
			double completionTimeSec = diff - (completionTimeMin * 60);
			
			System.out.println("Reasoner completed in " + completionTimeMin + 
					" mins and " + completionTimeSec + " secs");
			System.out.println("Printing diff results....");
			writeDiffResults(ontology, reasonerELK, resultStore, idReader);
			System.out.println("Done");
		}
		finally {
			resultStore.disconnect();
			idReader.disconnect();
			if(reasonerELK != null) 
				reasonerELK.dispose();
		}
	}

}
