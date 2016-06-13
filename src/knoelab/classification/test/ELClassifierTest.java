package knoelab.classification.test;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.GregorianCalendar;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Scanner;
import java.util.Set;

import knoelab.classification.init.AxiomDistributionType;
import knoelab.classification.init.EntityType;
import knoelab.classification.misc.Constants;
import knoelab.classification.misc.HostInfo;
import knoelab.classification.misc.PropertyFileHandler;
import knoelab.classification.misc.Util;

import org.semanticweb.elk.owlapi.ElkReasonerFactory;
import org.semanticweb.owlapi.apibinding.OWLManager;
import org.semanticweb.owlapi.model.AddAxiom;
import org.semanticweb.owlapi.model.IRI;
import org.semanticweb.owlapi.model.OWLClass;
import org.semanticweb.owlapi.model.OWLClassExpression;
import org.semanticweb.owlapi.model.OWLDataHasValue;
import org.semanticweb.owlapi.model.OWLLogicalAxiom;
import org.semanticweb.owlapi.model.OWLNamedIndividual;
import org.semanticweb.owlapi.model.OWLObjectHasValue;
import org.semanticweb.owlapi.model.OWLObjectIntersectionOf;
import org.semanticweb.owlapi.model.OWLObjectOneOf;
import org.semanticweb.owlapi.model.OWLObjectSomeValuesFrom;
import org.semanticweb.owlapi.model.OWLOntology;
import org.semanticweb.owlapi.model.OWLOntologyManager;
import org.semanticweb.owlapi.model.OWLSubClassOfAxiom;
import org.semanticweb.owlapi.reasoner.InferenceType;
import org.semanticweb.owlapi.reasoner.OWLReasoner;
import org.semanticweb.owlapi.reasoner.OWLReasonerFactory;
import org.semanticweb.owlapi.util.OWLOntologyMerger;
import org.semanticweb.owlapi.util.SimpleIRIMapper;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;
import uk.ac.manchester.cs.factplusplus.owlapiv3.FaCTPlusPlusReasonerFactory;
import au.csiro.snorocket.owlapi.SnorocketReasonerFactory;

import com.clarkparsia.modularity.IncrementalClassifier;
import com.clarkparsia.owlapiv3.OWL;
import com.clarkparsia.pellet.owlapiv3.PelletReasoner;
import com.clarkparsia.pellet.owlapiv3.PelletReasonerFactory;

import de.tudresden.inf.lat.jcel.owlapi.main.JcelReasoner;
import eu.trowl.owlapi3.rel.reasoner.el.RELReasoner;
import eu.trowl.owlapi3.rel.reasoner.el.RELReasonerFactory;
//import org.semanticweb.HermiT.Reasoner;


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
		precomputeAndCheckResults(ontology);
	}
	
	private void precomputeAndCheckResults(OWLOntology ontology) throws Exception {		
	    System.out.println("Not Normalizing");
		
	    PropertyFileHandler propertyFileHandler = PropertyFileHandler.getInstance();
	    HostInfo resultNodeHostInfo = propertyFileHandler.getResultNode();
	    // port: 6489 for snapshot testing
	    Jedis resultStore = new Jedis(resultNodeHostInfo.getHost(), 
	    		resultNodeHostInfo.getPort(), Constants.INFINITE_TIMEOUT);
	    Jedis resultStore2 = new Jedis(resultNodeHostInfo.getHost(), 
	    		resultNodeHostInfo.getPort(), Constants.INFINITE_TIMEOUT);
	    resultStore2.select(2);
	    HostInfo localHostInfo = propertyFileHandler.getLocalHostInfo();
	    Jedis localStore = new Jedis(localHostInfo.getHost(), localHostInfo.getPort());
	    Set<String> idHosts = 
	    		localStore.zrange(AxiomDistributionType.CONCEPT_ID.toString(), 
	    				Constants.RANGE_BEGIN, Constants.RANGE_END);
	    // currently there is only one ID node
	    String[] idHostPort = idHosts.iterator().next().split(":");
	    Jedis idReader = new Jedis(idHostPort[0], 
	    		Integer.parseInt(idHostPort[1]), Constants.INFINITE_TIMEOUT);
	    GregorianCalendar cal1 = new GregorianCalendar();
	    try {
//		    OWLReasonerFactory reasonerFactory = new ElkReasonerFactory();
//		    OWLReasoner reasoner = reasonerFactory.createReasoner(ontology);
//		    reasoner.precomputeInferences(InferenceType.CLASS_HIERARCHY);
//		    PelletReasoner reasoner = PelletReasonerFactory.getInstance().
//		    									createReasoner( ontology );
//		    reasoner.prepareReasoner();	    
	    	
	//	    Reasoner hermitReasoner = new Reasoner(ontology);
	//	    hermitReasoner.precomputeInferences(InferenceType.CLASS_HIERARCHY);
	    	
//	    	OWLReasonerFactory reasonerFactory = new ElkReasonerFactory();
//		    OWLReasoner reasoner = reasonerFactory.createReasoner(ontology);
//		    reasoner.precomputeInferences(InferenceType.CLASS_HIERARCHY);
	    	
//		    RELReasonerFactory relfactory = new RELReasonerFactory();
//		    RELReasoner reasoner = relfactory.createReasoner(ontology);
//		    reasoner.precomputeInferences(InferenceType.CLASS_HIERARCHY);
	    	
//	    	JcelReasoner reasoner = new JcelReasoner(ontology, false);
//		    reasoner.precomputeInferences(InferenceType.CLASS_HIERARCHY);
	    	
	    	OWLReasonerFactory reasonerFactory = 
					new ElkReasonerFactory();
			OWLReasoner reasoner = reasonerFactory.createReasoner(
					ontology);
			reasoner.precomputeInferences(
					InferenceType.CLASS_HIERARCHY);
	    	
		    System.out.println("Reasoner completed in (millis): " + 
		    		Util.getElapsedTime(cal1));
		    
		    System.out.println("Comparing results using ELK.....");	    
		    rearrangeAndCompareResults(ontology, reasoner, 
		    		resultStore, resultStore2, idReader);
		
//		    pelletReasoner.dispose();
//	   		reasonerELK.dispose();
		    reasoner.dispose();
	    }
	    finally {
	    	localStore.disconnect();
		    resultStore.disconnect();
		    resultStore2.disconnect();
		    idReader.disconnect();
	    }
	}			
	
	private Set<String> getSuperClasses(OWLReasoner reasoner, OWLClass cl, 
			OWLClass owlThing) {
		Set<String> pset = new HashSet<String>();
		Set<OWLClass> reasonerSuperClasses = reasoner.getSuperClasses(cl, 
				false).getFlattened();
		// add cl itself to S(X) computed by reasoner. That is missing
		// in its result.
		reasonerSuperClasses.add(cl);
		reasonerSuperClasses.add(owlThing);
		// adding equivalent classes -- they are not considered if asked for superclasses
		Iterator<OWLClass> iterator = reasoner.getEquivalentClasses(cl).iterator();
		while(iterator.hasNext())
			reasonerSuperClasses.add(iterator.next());
		for(OWLClass scl : reasonerSuperClasses)
			pset.add(scl.toString());
		return pset;
	}
	
	public void getReasonerRunTime(String ontPath) throws Exception {
			
		Scanner scanner = new Scanner(System.in);
		System.out.println("Select a reasoner");
		System.out.println("\t 1 ELK \n\t 2 jCEL \n\t 3 Snorocket");
		System.out.println("\t 4 Pellet \n\t 5 HermiT \n\t 6 JFact");
		System.out.println("Enter your choice: ");
		int option = scanner.nextInt();
		scanner.close();
		
		File ontFile = new File(ontPath);
		IRI documentIRI = IRI.create(ontFile);
		OWLOntologyManager manager = OWLManager.createOWLOntologyManager();	
		System.out.println("Loading ontology ...");
		OWLOntology ontology = manager.loadOntologyFromOntologyDocument(documentIRI);
		
		OWLClass owlThing = ontology.getOWLOntologyManager().
				getOWLDataFactory().getOWLThing();
		System.out.println("Assuming the input ontology is already normalized");
		GregorianCalendar start = new GregorianCalendar();
		
		switch(option) {
			case 1:	
					System.out.println("Using ELK...");
//					PrintWriter elkWriter = new PrintWriter(new BufferedWriter(
//					        new FileWriter("final-saxioms-elk.txt")));
					long startTime1 = System.nanoTime();
					OWLReasonerFactory reasonerFactory = 
							new ElkReasonerFactory();
					OWLReasoner reasoner = reasonerFactory.createReasoner(
							ontology);
					reasoner.precomputeInferences(
							InferenceType.CLASS_HIERARCHY);
//					System.out.println("Writing results to file ...");
//					Set<OWLClass> ontConcepts = ontology.getClassesInSignature();
//					for(OWLClass concept : ontConcepts) {
//						Set<String> superClasses = getSuperClasses(
//								reasoner, concept, owlThing);
//						for(String superClass : superClasses)
//							elkWriter.println(concept.toString() + "|" + superClass);
//					}
					reasoner.dispose();
					double totalTime1 = Util.getElapsedTimeSecs(startTime1);
					System.out.println("ELK - time (secs): " + totalTime1);
//					elkWriter.close();
					break;
					
			case 2: 
					System.out.println("Using jCEL...");
					long startTime2 = System.nanoTime();
					JcelReasoner jcelReasoner = new JcelReasoner(
							ontology, false);
				    jcelReasoner.precomputeInferences(
				    		InferenceType.CLASS_HIERARCHY);
				    jcelReasoner.dispose();
				    double totalTime2 = Util.getElapsedTimeSecs(startTime2);
					System.out.println("jcel - time (secs): " + totalTime2);
					break;
					
			case 3:
					System.out.println("Using Snorocket...");
					long startTime3 = System.nanoTime();
					SnorocketReasonerFactory srf = 
							new SnorocketReasonerFactory();
				    OWLReasoner snorocketReasoner = 
				    		srf.createNonBufferingReasoner(ontology);
				    snorocketReasoner.precomputeInferences(
				    		InferenceType.CLASS_HIERARCHY);
				    snorocketReasoner.dispose();
				    double totalTime3 = Util.getElapsedTimeSecs(startTime3);
					System.out.println("Snorocket - time (secs): " + totalTime3);
					break;
					
			case 4:
					System.out.println("Using Pellet...");
					PrintWriter writer = new PrintWriter(new BufferedWriter(
				        new FileWriter("final-saxioms-pellet.txt")));
					PelletReasoner pelletReasoner = 
					    	PelletReasonerFactory.getInstance().createReasoner(
					    			ontology);
					pelletReasoner.prepareReasoner();	
					pelletReasoner.precomputeInferences(
							InferenceType.CLASS_HIERARCHY);
					Set<OWLClass> concepts = ontology.getClassesInSignature();
					for(OWLClass concept : concepts) {
						Set<String> superClasses = getSuperClasses(
								pelletReasoner, concept, owlThing);
						for(String superClass : superClasses)
							writer.println(concept.toString() + "|" + superClass);
					}
					pelletReasoner.dispose();
					writer.close();
					break;
					
			case 5:
					System.out.println("Using HermiT...");
//					Reasoner hermitReasoner = new Reasoner(ontology);
//				    hermitReasoner.precomputeInferences(
//				    		InferenceType.CLASS_HIERARCHY);
//				    hermitReasoner.dispose();
					break;
					
			case 6:
				//JFact jar gave java version problems. Using Fact++, connects via JNI.
				//use -Djava.library.path=./FaCT++-linux-v1.6.2/64bit as arg. 
				//java <heap> -Djava.library.path=./FaCT++-linux-v1.6.2/64bit ...
					OWLReasoner factppReasoner = new FaCTPlusPlusReasonerFactory().
						createReasoner(ontology);
					factppReasoner.precomputeInferences(
						InferenceType.CLASS_HIERARCHY);
					factppReasoner.dispose();
					break;
			default:
					throw new Exception("Wrong option given");
		}
		System.out.println("Time taken (millis): " + 
				Util.getElapsedTime(start));
	}
	
	public void getELKRunTime(String ontPath) throws Exception {
		File ontFile = new File(ontPath);
		IRI documentIRI = IRI.create(ontFile);
		System.out.println("using ELK ...");
		OWLOntologyManager manager = OWLManager.createOWLOntologyManager();	
		System.out.println("Loading ontology ...");
		Long beginTime = System.nanoTime();
		OWLOntology ontology = manager.loadOntologyFromOntologyDocument(documentIRI);
		OWLReasonerFactory reasonerFactory = 
				new ElkReasonerFactory();
		OWLReasoner reasoner = reasonerFactory.createReasoner(
				ontology);
		reasoner.precomputeInferences(
				InferenceType.CLASS_HIERARCHY);
		reasoner.dispose();
		Long endTime = System.nanoTime();
		System.out.println("Time taken for classification (seconds): " + 
					(endTime - beginTime)/1e9);
	}
	
	private void getPelletIncrementalClassifierRunTime(String baseOnt, 
			String ontDir) throws Exception {
		System.out.println("Using Pellet Incremental Classifier...");
		GregorianCalendar start = new GregorianCalendar();
		File ontFile = new File(baseOnt);
		IRI documentIRI = IRI.create(ontFile);
		OWLOntology baseOntology = OWL.manager.loadOntology(documentIRI);
		IncrementalClassifier classifier = new IncrementalClassifier( baseOntology );
		classifier.classify();
		System.out.println("Logical axioms: " + baseOntology.getLogicalAxiomCount());
		System.out.println("Time taken for base ontology (millis): " + 
				Util.getElapsedTime(start));
		File ontDirPath = new File(ontDir);
		File[] allFiles = ontDirPath.listFiles();
		OWLOntologyManager manager = OWLManager.createOWLOntologyManager();
		addTripsBaseOntologies(manager);
		int count = 1;
		for(File owlFile : allFiles) {
			IRI owlDocumentIRI = IRI.create(owlFile);
			OWLOntology ontology = manager.loadOntologyFromOntologyDocument(
										owlDocumentIRI);
			Set<OWLLogicalAxiom> axioms = ontology.getLogicalAxioms();
			for(OWLLogicalAxiom axiom : axioms)
				OWL.manager.applyChange( new AddAxiom(baseOntology, axiom) );
			
			System.out.println("\nLogical axioms: " + baseOntology.getLogicalAxiomCount());
			System.out.println(count + "  file: " + owlFile.getName());
//			System.out.println("" + count + "  file: " + owlFile.getName());
//			GregorianCalendar start2 = new GregorianCalendar();
	        classifier.classify();
//	        System.out.println("Time taken (millis): " + 
//					Util.getElapsedTime(start2));
			manager.removeOntology(ontology);
			count++;
		}
		System.out.println("\nTotal time taken (millis): " + Util.getElapsedTime(start));
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
			Set<OWLClass> reasonerSuperclasses = reasoner.getSuperClasses(cl, 
					false).getFlattened();
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
	
	private void rearrangeAndCompareResults(OWLOntology ontology, 
			OWLReasoner reasoner, Jedis resultStore, Jedis resultStore2, Jedis idReader) 
	throws Exception {
		new ResultRearranger().initializeAndRearrange();
		Set<OWLClass> classes = ontology.getClassesInSignature();
		//rearranged results are in DB-1
		resultStore.select(1);
		double classCount = 0;
		int multiplier = 1;
		int missCount = 0;
		String bottomID = Util.getPackedID(Constants.BOTTOM_ID, EntityType.CLASS);
		System.out.println("Comparing Classes... " + classes.size());
		OWLClass owlThing = ontology.getOWLOntologyManager().
								getOWLDataFactory().getOWLThing();
		for (OWLClass cl : classes) {
			String classID = conceptToID(cl.toString(), idReader);
			//REL/Pellet doesn't consider individuals i.e. {a} \sqsubseteq \bottom
			// so skipping checking bottom
			if(classID.equals(bottomID))
				continue;
			
			classCount++;
			Set<OWLClass> reasonerSuperClasses = reasoner.getSuperClasses(cl, 
					false).getFlattened();
			// add cl itself to S(X) computed by reasoner. That is missing
			// in its result.
			reasonerSuperClasses.add(cl);
			reasonerSuperClasses.add(owlThing);
			// adding equivalent classes -- they are not considered if asked for superclasses
			Iterator<OWLClass> iterator = reasoner.getEquivalentClasses(cl).iterator();
			while(iterator.hasNext())
				reasonerSuperClasses.add(iterator.next());
			Set<String> superClasses = resultStore.smembers(classID);
			if(superClasses.size() == reasonerSuperClasses.size()) {
				compareAndPrintEqualSizedClasses(cl, reasonerSuperClasses, 
						superClasses, idReader);
			}
			else {
				System.out.println("\n" + cl.toString() + " -- " + 
						superClasses.size() + ", " + reasonerSuperClasses.size());
				for(OWLClass scl : reasonerSuperClasses) {
					String sclID = conceptToID(scl.toString(), idReader);
					if(!superClasses.contains(sclID)) {
						System.out.print(cl.toString() + " -ne- " + 
								scl.toString());
						System.out.print("  ,  ");
					}
					superClasses.remove(sclID);
				}
				for(String s : superClasses)
					System.out.println("\t -- " + Util.idToConcept(s, idReader) + 
							"(" + s + ")");
				System.out.println();
				missCount++;
			}
		}
		System.out.println("No of classes not equal: " + missCount);
		
		Set<OWLNamedIndividual> individuals = ontology.getIndividualsInSignature();
		System.out.println("Rearranging individuals...");
		System.out.println("Individuals: " + individuals.size());
		System.out.println("Not checking for individuals...");
/*		
		rearrangeIndividuals(individuals, resultStore, resultStore2, idReader);
		int cnt = 0;
		for(OWLClass cl : classes) {	
			Set<OWLNamedIndividual> instances = 
				reasoner.getInstances(cl, false).getFlattened();
			Set<String> computedInstances = resultStore2.smembers(
					conceptToID(cl.toString(), idReader));
			if(computedInstances.size() == instances.size()) {
				compareAndPrintEqualSizedIndividuals(cl, instances, computedInstances, idReader);
			}
			else {
				System.out.println(cl.toString() + " -- " + 
						computedInstances.size() + " , " + instances.size());
				compareAndPrintUnEqualSizedIndividuals(cl, instances, computedInstances, idReader);
				cnt++;
			}
		}
		System.out.println("No of classes for which individuals didn't match: " + cnt);
*/		
		resultStore.select(0);
	}
	
	private void writeResultsToFile() throws Exception {
		PropertyFileHandler propertyFileHandler = PropertyFileHandler.getInstance();
	    HostInfo resultNodeHostInfo = propertyFileHandler.getResultNode();
	    // port: 6489 for snapshot testing
	    Jedis resultStore = new Jedis(resultNodeHostInfo.getHost(), 
	    		resultNodeHostInfo.getPort(), Constants.INFINITE_TIMEOUT);
		new ResultRearranger().initializeAndRearrange();
		//rearranged results are in DB-1
		resultStore.select(1);
		Set<String> resultKeys = resultStore.smembers(Constants.RESULT_KEYS);
		PrintWriter writer = new PrintWriter(new BufferedWriter(
		        new FileWriter("final-saxioms-distel.txt")));
		System.out.println("Writing results to final-saxioms-distel.txt");
		
		for(String key : resultKeys) {
			Set<String> superClasses = resultStore.smembers(key);
			for(String sc : superClasses) 
				writer.println(key + "|" + sc);
		}
		writer.close();
		resultStore.close();
	}
	
	private void compareAndPrintEqualSizedClasses(OWLClass cl,
			Set<OWLClass> reasonerSuperClasses, Set<String> superClasses, 
			Jedis idReader) throws Exception {
		//compare each element of these 2 sets
		boolean print = false;
		for(OWLClass scl : reasonerSuperClasses) {
			String sclID = conceptToID(scl.toString(), idReader);
			if(!superClasses.contains(sclID)) {
				print = true;
				System.out.print(cl.toString() + " -e- " + 
						scl.toString());
				System.out.print("  ,  ");
			}
		}
		if(print)
			System.out.println("\n");
	}
	
	private void compareAndPrintEqualSizedIndividuals(OWLClass cl,
			Set<OWLNamedIndividual> reasonerInstances, Set<String> computedInstances, 
			Jedis idReader) throws Exception {
		//compare each element of these 2 sets
		boolean print = false;
		for(OWLNamedIndividual scl : reasonerInstances) {
			String sclID = conceptToID(scl.toString(), idReader);
			if(!computedInstances.contains(sclID)) {
				print = true;
				System.out.print(cl.toString() + " -e- " + 
						scl.toString());
				System.out.print("  ,  ");
			}
		}
		if(print)
			System.out.println("\n");
	}
	
	private void compareAndPrintUnEqualSizedIndividuals(OWLClass cl,
			Set<OWLNamedIndividual> reasonerInstances, Set<String> computedInstances, 
			Jedis idReader) throws Exception {
		//compare each element of these 2 sets
		boolean print = false;
		for(OWLNamedIndividual scl : reasonerInstances) {
			String sclID = conceptToID(scl.toString(), idReader);
			if(!computedInstances.contains(sclID)) {
				print = true;
				System.out.print(cl.toString() + " -ne- " + 
						scl.toString());
				System.out.print("  ,  ");
			}
			computedInstances.remove(sclID);
		}
		for(String s : computedInstances)
			System.out.println("\t -- " + Util.idToConcept(s, idReader) + 
					"(" + s + ")");
		System.out.println();
	}
	
	private void rearrangeIndividuals(Set<OWLNamedIndividual> individuals, 
			Jedis resultStore, Jedis resultStore2, Jedis idReader) throws Exception {
		resultStore2.flushDB();
		Pipeline p = resultStore2.pipelined();
		double cnt = 0;
		int multiplier = 1;
		for(OWLNamedIndividual individual : individuals) {
			String indID = conceptToID(individual.toString(), idReader);
			Set<String> classInstances = resultStore.smembers(indID);
			for(String cl : classInstances) 
				if(!cl.equals(indID))
					p.sadd(cl, indID);
			cnt++;
			double keyProgress = (cnt/individuals.size())*100;
			if(keyProgress >= (5*multiplier)) {
				System.out.println("% of no. of keys rearranged: " + keyProgress);
				multiplier++;
				p.sync();
			}
		}
		p.sync();
	}
	
	public void mergeAndCompare(String dirPath) throws Exception {
		File dir = new File(dirPath);
		File[] files = dir.listFiles();
		OWLOntologyManager manager = OWLManager.createOWLOntologyManager();
		OWLOntologyMerger merger = new OWLOntologyMerger(manager);
		for(File f : files) 
			manager.loadOntologyFromOntologyDocument(IRI.create(f));
		String s = "norm-merged-base+300.owl";
		IRI iri = IRI.create(new File(s));
		OWLOntology mergedOntology = 
			merger.createMergedOntology(manager, iri);
		manager.saveOntology(mergedOntology, iri);
		System.out.println("Done creating merged ontology");
//		precomputeAndCheckResults(mergedOntology);
	}
	
	private String conceptToID(String concept, Jedis idReader) throws Exception {
		String conceptID = idReader.get(concept);
		if(conceptID == null)
			throw new Exception("Concept does not exist in DB: " + concept);
		return conceptID;
	}
	
	public void getELKIncrementalRuntime(String baseOnt, String ontDir) 
			throws Exception {
		GregorianCalendar start = new GregorianCalendar();
		OWLOntologyManager manager = OWLManager.createOWLOntologyManager();
		IRI documentIRI = IRI.create(new File(baseOnt));
		OWLOntology baseOntology = manager.loadOntology(documentIRI);
		System.out.println("Logical axioms: " + baseOntology.getLogicalAxiomCount());
//		findDataHasValueAxiom(baseOntology.getAxioms(AxiomType.SUBCLASS_OF));

		OWLReasonerFactory reasonerFactory = new ElkReasonerFactory();
	    OWLReasoner reasoner = reasonerFactory.createReasoner(baseOntology);
	    reasoner.precomputeInferences(InferenceType.CLASS_HIERARCHY);
	    File[] files = new File(ontDir).listFiles();
	    int count = 0;
	    OWLOntologyManager manager2 = OWLManager.createOWLOntologyManager();
	    addTripsBaseOntologies(manager2);
	    for(File file : files) {
	    	System.out.println("File name: " + file.getName());
	    	documentIRI = IRI.create(file);
	    	OWLOntology ontology = manager2.loadOntology(documentIRI);
	    	Set<OWLLogicalAxiom> axioms = ontology.getLogicalAxioms();
//	    	findDataHasValueAxiom(ontology.getAxioms(AxiomType.SUBCLASS_OF));
	    	manager.addAxioms(baseOntology, axioms);
	    	reasoner.flush();
	    	System.out.println("Logical axioms: " + baseOntology.getLogicalAxiomCount());
	    	
	    	// From the ELK wiki, it seems ABox reasoning will trigger TBox reasoning
	    	reasoner.precomputeInferences(InferenceType.CLASS_ASSERTIONS);
	    	
	    	manager2.removeOntology(ontology);
	    	count++;
	    	System.out.println("Done with " + count);
//	    	if(count == 5)
//	    		break;
	    }	    
	    reasoner.dispose();
	    System.out.println("Time taken (millis): " + 
				Util.getElapsedTime(start));
	}
/*	
	public void getHermitIncrementalRuntime(String baseOnt, String ontDir) 
			throws Exception {
		GregorianCalendar start = new GregorianCalendar();
		OWLOntologyManager manager = OWLManager.createOWLOntologyManager();
		IRI documentIRI = IRI.create(new File(baseOnt));
		OWLOntology baseOntology = manager.loadOntology(documentIRI);
		System.out.println("Logical axioms: " + baseOntology.getLogicalAxiomCount());
		
		Reasoner hermitReasoner = new Reasoner(baseOntology);
		hermitReasoner.precomputeInferences(InferenceType.CLASS_HIERARCHY);
	    File[] files = new File(ontDir).listFiles();
	    int count = 0;
	    OWLOntologyManager manager2 = OWLManager.createOWLOntologyManager();
	    addTripsBaseOntologies(manager2);
	    for(File file : files) {
	    	System.out.println("\nFile name: " + file.getName());
	    	documentIRI = IRI.create(file);
	    	OWLOntology ontology = manager2.loadOntology(documentIRI);
	    	Set<OWLLogicalAxiom> axioms = ontology.getLogicalAxioms();
	    	manager.addAxioms(baseOntology, axioms);
	    	hermitReasoner.flush();
	    	System.out.println("Logical axioms: " + baseOntology.getLogicalAxiomCount());
	    	
	    	// ABox reasoning should trigger TBox reasoning?
	    	hermitReasoner.precomputeInferences(InferenceType.CLASS_ASSERTIONS);
	    	
	    	manager2.removeOntology(ontology);
	    	count++;
	    	System.out.println("Done with " + count);
//	    	if(count == 5)
//	    		break;
	    }	    
	    hermitReasoner.dispose();
	    System.out.println("\nTime taken (millis): " + 
				Util.getElapsedTime(start));
	}
*/	
	
	private void findDataHasValueAxiom(Set<OWLSubClassOfAxiom> axioms) {
		for(OWLSubClassOfAxiom ax : axioms) {
			OWLClassExpression oce = ax.getSuperClass();
			if(oce instanceof OWLDataHasValue) {
				System.out.println(ax + "  isAcceptableType: " + 
						isAcceptableType(oce));
				break;
			}
		}
	}
	
	private boolean isAcceptableType(OWLClassExpression oce) {
		boolean isAcceptableType = false;
		if(oce instanceof OWLClass)
			isAcceptableType = true;
		else if(oce instanceof OWLObjectOneOf) {
			if(((OWLObjectOneOf) oce).getIndividuals().size() > 1) 
				isAcceptableType = false;
			else
				isAcceptableType = true;
		}
		else if(oce instanceof OWLObjectSomeValuesFrom)
			isAcceptableType = true;
		else if(oce instanceof OWLObjectHasValue)
			isAcceptableType = true;
		else if(oce instanceof OWLObjectIntersectionOf)
			isAcceptableType = true;
		
		return isAcceptableType;
	}
	
	private void addTripsBaseOntologies(OWLOntologyManager manager) 
			throws Exception {
				SimpleIRIMapper iriMapperTime = new SimpleIRIMapper(
						IRI.create("http://www.w3.org/2006/time"), 
						IRI.create(new File("Time.owl")));
				manager.addIRIMapper(iriMapperTime);		
				SimpleIRIMapper iriMapperCore = new SimpleIRIMapper(
						IRI.create("http://www.ibm.com/SCTC/ontology/" +
									"CoreSpatioTemporalDataSensorOntology.owl"),
						IRI.create(new File("CoreSpatioTemporalDataSensorOntology.owl")));
				manager.addIRIMapper(iriMapperCore);
				SimpleIRIMapper iriMapperTravelTime = new SimpleIRIMapper(
						IRI.create("http://www.ibm.com/SCTC/ontology/TravelTimeOntology.owl"), 
						IRI.create(new File("TravelTimeOntology.owl")));
				manager.addIRIMapper(iriMapperTravelTime);
	}
	
	public static void main(String[] args) throws Exception {
		if(args.length != 1) {
			System.out.println("Give the path of owl file");
    		System.exit(-1);
		}
//		new ELClassifierTest().precomputeAndCheckResults(args);
//		new ELClassifierTest().mergeAndCompare(args[0]);
		new ELClassifierTest().getReasonerRunTime(args[0]);
//		new ELClassifierTest().getELKRunTime(args[0]);
//		new ELClassifierTest().writeResultsToFile();
//		new ELClassifierTest().getELKIncrementalRuntime(args[0], args[1]);
//		new ELClassifierTest().getPelletIncrementalClassifierRunTime(
//				args[0], args[1]);
//		new ELClassifierTest().getHermitIncrementalRuntime(args[0], args[1]);
	}

}
