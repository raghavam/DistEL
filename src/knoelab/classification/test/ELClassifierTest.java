package knoelab.classification.test;

import java.io.File;
import java.util.ArrayList;
import java.util.GregorianCalendar;
import java.util.Iterator;
import java.util.List;
import java.util.Scanner;
import java.util.Set;

import knoelab.classification.init.AxiomDistributionType;
import knoelab.classification.misc.Constants;
import knoelab.classification.misc.HostInfo;
import knoelab.classification.misc.PropertyFileHandler;
import knoelab.classification.misc.Util;

import org.semanticweb.HermiT.Reasoner;
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
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;

import com.clarkparsia.pellet.owlapiv3.PelletReasoner;
import com.clarkparsia.pellet.owlapiv3.PelletReasonerFactory;

import de.tudresden.inf.lat.jcel.owlapi.main.JcelReasoner;


/**
 * This class compares the classification output of Pellet (for a
 * few Ontologies) with my ELClassifier results.
 *  
 * @author Raghava
 */
public class ELClassifierTest {

	public void precomputeAndCheckResults(String[] args) throws Exception {		
		OWLOntologyManager manager = OWLManager.createOWLOntologyManager();
		
		System.out.println("Comparing classification output for " + args[0]);
		File ontFile = new File(args[0]);
		IRI documentIRI = IRI.create(ontFile);
		OWLOntology ontology = manager.loadOntologyFromOntologyDocument(documentIRI);
	    System.out.println("Not Normalizing");
		
	    PropertyFileHandler propertyFileHandler = PropertyFileHandler.getInstance();
	    HostInfo resultNodeHostInfo = propertyFileHandler.getResultNode();
	    // port: 6489 for snapshot testing
	    Jedis resultStore = new Jedis(resultNodeHostInfo.getHost(), 
	    		resultNodeHostInfo.getPort(), Constants.INFINITE_TIMEOUT);
	    // TODO: Don't hardcode the ID store
	    HostInfo localHostInfo = propertyFileHandler.getLocalHostInfo();
	    Jedis localStore = new Jedis(localHostInfo.getHost(), localHostInfo.getPort());
	    Set<String> idHosts = 
	    		localStore.smembers(AxiomDistributionType.CONCEPT_ID.toString());
	    // currently there is only one ID node
	    String[] idHostPort = idHosts.iterator().next().split(":");
	    Jedis idReader = new Jedis(idHostPort[0], 
	    		Integer.parseInt(idHostPort[1]), Constants.INFINITE_TIMEOUT);
	    GregorianCalendar cal1 = new GregorianCalendar();
	    OWLReasonerFactory reasonerFactory = new ElkReasonerFactory();
	    OWLReasoner reasonerELK = reasonerFactory.createReasoner(ontology);
	    reasonerELK.precomputeInferences(InferenceType.CLASS_HIERARCHY);
//	    PelletReasoner pelletReasoner = PelletReasonerFactory.getInstance().createReasoner( ontology );
//	    pelletReasoner.prepareReasoner();	    
//	    Reasoner hermitReasoner = new Reasoner(ontology);
//	    hermitReasoner.precomputeInferences(InferenceType.CLASS_HIERARCHY);
	    
	    System.out.print("Reasoner completed in ");
	    Util.printElapsedTime(cal1);
	    
	    System.out.println("Comparing results using ELK.....");
	    compareClassificationResults(ontology, reasonerELK, 
	    		resultStore, idReader);
	
//	    pelletReasoner.dispose();
	    reasonerELK.dispose();
	    resultStore.disconnect();
	    idReader.disconnect();
	}
	
	public void getReasonerRunTime(String ontPath) throws Exception {
		OWLOntologyManager manager = OWLManager.createOWLOntologyManager();
		
		System.out.println("Comparing classification output for " + ontPath);
		File ontFile = new File(ontPath);
		IRI documentIRI = IRI.create(ontFile);
		OWLOntology ontology = manager.loadOntologyFromOntologyDocument(documentIRI);
	    System.out.println("Not Normalizing");
	    System.out.println("Excluding the time required to load ontology by OWL API");
	    GregorianCalendar cal1 = new GregorianCalendar();
//	    OWLReasonerFactory reasonerFactory = new ElkReasonerFactory();
//	    OWLReasoner reasonerELK = reasonerFactory.createReasoner(ontology);
//	    reasonerELK.precomputeInferences(InferenceType.CLASS_HIERARCHY);
	    
//	    JcelReasoner jcelReasoner = new JcelReasoner(ontology, false);
//	    jcelReasoner.precomputeInferences(InferenceType.CLASS_HIERARCHY);
	    
	    PelletReasoner pelletReasoner = 
	    	PelletReasonerFactory.getInstance().createReasoner( ontology );
	    pelletReasoner.prepareReasoner();	
	    pelletReasoner.precomputeInferences(InferenceType.CLASS_HIERARCHY);
	    
	    System.out.print("Reasoner completed in ");
//	    reasonerELK.dispose();
//	    jcelReasoner.dispose();
	    pelletReasoner.dispose();
	    Util.printElapsedTime(cal1);
	}
	
	private void compareClassificationResults(OWLOntology ontology, 
			OWLReasoner reasoner, Jedis resultStore, Jedis idReader) 
			throws Exception {
		Set<OWLClass> classes = ontology.getClassesInSignature();
		Pipeline resultPipeline = resultStore.pipelined();
		double classCount = 0;
		int multiplier = 1;
		double totalCount = 0;
		for (OWLClass cl : classes) {
			classCount++;
			double classProgress = (classCount/classes.size())*100;
			Set<OWLClass> reasonerSuperclasses = reasoner.getSuperClasses(cl, false).getFlattened();
			// add cl itself to S(X) computed by reasoner. That is missing
			// in its result.
			reasonerSuperclasses.add(cl);
			// adding equivalent classes -- they are not considered if asked for superclasses
			Iterator<OWLClass> iterator = reasoner.getEquivalentClasses(cl).iterator();
			while(iterator.hasNext())
				reasonerSuperclasses.add(iterator.next());
			String classToCheckID = conceptToID(cl.toString(), idReader);
			List<Response<Double>> responseList = new ArrayList<Response<Double>>();
			for(OWLClass scl : reasonerSuperclasses) {
				String key = conceptToID(scl.toString(), idReader);
				responseList.add(resultPipeline.zscore(key, classToCheckID));
			}
			resultPipeline.sync();
			double hitCount = 0;
			for(Response<Double> response : responseList) {
				if(response.get() != null)
					hitCount++;
			}
			totalCount += (hitCount/reasonerSuperclasses.size());
			if(classProgress >= (5*multiplier)) {
				System.out.println("% of no. of classes looked at: " + classProgress + 
						"\tProgress %: " + (totalCount/classCount)*100);
				multiplier++;
			}
		}
		double progress = totalCount/classes.size();
		System.out.println("\nProgress %: " + (progress*100));
	}
	
	private String conceptToID(String concept, Jedis idReader) throws Exception {
		String conceptID = idReader.get(concept);
		if(conceptID == null)
			throw new Exception("Concept does not exist in DB: " + concept);
		return conceptID;
	}
	
	public static void main(String[] args) throws Exception {
		if(args.length != 1 || args[0].isEmpty()) {
			System.out.println("Give the path of owl file");
    		System.exit(-1);
		}
//		new ELClassifierTest().precomputeAndCheckResults(args);
		new ELClassifierTest().getReasonerRunTime(args[0]);
	}

}
