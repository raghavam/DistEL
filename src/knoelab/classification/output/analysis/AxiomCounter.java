package knoelab.classification.output.analysis;

import java.io.File;
import java.util.HashSet;
import java.util.Set;

import knoelab.classification.init.AxiomDistributionType;
import knoelab.classification.misc.AxiomDB;
import knoelab.classification.misc.Constants;
import knoelab.classification.misc.HostInfo;
import knoelab.classification.misc.PropertyFileHandler;

import org.semanticweb.owlapi.apibinding.OWLManager;
import org.semanticweb.owlapi.model.IRI;
import org.semanticweb.owlapi.model.OWLAxiom;
import org.semanticweb.owlapi.model.OWLClass;
import org.semanticweb.owlapi.model.OWLClassExpression;
import org.semanticweb.owlapi.model.OWLLogicalAxiom;
import org.semanticweb.owlapi.model.OWLObjectSomeValuesFrom;
import org.semanticweb.owlapi.model.OWLOntology;
import org.semanticweb.owlapi.model.OWLOntologyManager;
import org.semanticweb.owlapi.model.OWLSubClassOfAxiom;

import redis.clients.jedis.Jedis;

/**
 * Functionality of this class is to count the number of logical axioms 
 * especially axioms of type A \sqsubseteq B and A \sqsubseteq \exists r.B
 * before and after classification. We can get the number of axioms added 
 * by classification.
 * 
 * @author Raghava
 */
public class AxiomCounter {

	/**
	 * Get number of logical axioms, A < B, A < 3r.B with and 
	 * without duplicates
	 */
	public void getAxiomCountBeforeClassification(String ontPath) 
			throws Exception {
		File ontPathFile = new File(ontPath);
		File[] ontFiles;
		if(ontPathFile.isDirectory()) 
			ontFiles = ontPathFile.listFiles();
		else if(ontPathFile.isFile()) 
			ontFiles = new File[]{ontPathFile};
		else
			throw new Exception("Unexpected type, path should be " +
					"either a file or directory");
		OWLOntologyManager manager = OWLManager.createOWLOntologyManager();
		Set<OWLLogicalAxiom> axiomsWithoutDup = new HashSet<OWLLogicalAxiom>();
		Set<OWLAxiom> subClassAxiomsWithoutDup = new HashSet<OWLAxiom>();
		Set<OWLAxiom> rsetAxiomsWithoutDup = new HashSet<OWLAxiom>();
		Set<OWLClass> classes = new HashSet<OWLClass>();
		int axiomsWithDup = 0;
		int subClassAxiomsWithDup = 0;
		int rsetAxiomsWithDup = 0;
		int numClassesWithDup = 0;
		int numfiles = 0;
		for(File ontFile : ontFiles) {
			IRI documentIRI = IRI.create(ontFile);
	        OWLOntology ontology = manager.loadOntology(documentIRI);
	        classes.addAll(ontology.getClassesInSignature());
	        numClassesWithDup += ontology.getClassesInSignature().size();
	        Set<OWLLogicalAxiom> axioms = ontology.getLogicalAxioms();
	        axiomsWithoutDup.addAll(axioms);
	        axiomsWithDup += axioms.size();
	        for(OWLLogicalAxiom ax : axioms) {
	        	if(ax instanceof OWLSubClassOfAxiom) {
	        		OWLSubClassOfAxiom subAxiom = (OWLSubClassOfAxiom)ax;
	        		OWLClassExpression subOCE = subAxiom.getSubClass();
	        		OWLClassExpression superOCE = subAxiom.getSuperClass();
	        		if(subOCE instanceof OWLClass) {
	        			if(superOCE instanceof OWLClass) {
	        				subClassAxiomsWithDup++;
	        				subClassAxiomsWithoutDup.add(ax);
	        			}
	        			else if(superOCE instanceof OWLObjectSomeValuesFrom) {
	        				rsetAxiomsWithDup++;
	        				rsetAxiomsWithoutDup.add(ax);
	        			}
	        		}
	        	}
	        }
	        manager.removeOntology(ontology);
	        numfiles++;
		}
		System.out.println("Num files: " + numfiles);
		System.out.println("Without duplicates: ");
		System.out.println("\t Logical Axioms: " + axiomsWithoutDup.size());
		System.out.println("\t Subclass Axioms: " + 
				subClassAxiomsWithoutDup.size());
		System.out.println("\t R(r) axioms: " + rsetAxiomsWithoutDup.size());
		System.out.println("\t Remaining axioms (add them to after " +
				"classification axiom count): " + 
				(axiomsWithoutDup.size() - 
						(subClassAxiomsWithoutDup.size() + 
								rsetAxiomsWithoutDup.size())));
		System.out.println("\t Classes: " + classes.size());
		System.out.println("\nWith duplicates: ");
		System.out.println("\t Logical Axioms: " + axiomsWithDup);
		System.out.println("\t Subclass Axioms: " + subClassAxiomsWithDup);
		System.out.println("\t R(r) axioms: " + rsetAxiomsWithDup);
		System.out.println("\t Classes: " + numClassesWithDup);
	}
	
	
	/**
	 * Get number of logical axioms, A < B, A < 3r.B with and 
	 * without duplicates
	 */
	public void getAxiomCountBeforeClassificationWithoutDupCheck(String ontPath) 
			throws Exception {
		File ontPathFile = new File(ontPath);
		File[] ontFiles;
		if(ontPathFile.isDirectory()) 
			ontFiles = ontPathFile.listFiles();
		else if(ontPathFile.isFile()) 
			ontFiles = new File[]{ontPathFile};
		else
			throw new Exception("Unexpected type, path should be " +
					"either a file or directory");
		OWLOntologyManager manager = OWLManager.createOWLOntologyManager();
		int axiomsWithDup = 0;
		int subClassAxiomsWithDup = 0;
		int rsetAxiomsWithDup = 0;
		int numClassesWithDup = 0;
		int numfiles = 0;
		for(File ontFile : ontFiles) {
			IRI documentIRI = IRI.create(ontFile);
	        OWLOntology ontology = manager.loadOntology(documentIRI);
	        numClassesWithDup += ontology.getClassesInSignature().size();
	        Set<OWLLogicalAxiom> axioms = ontology.getLogicalAxioms();
	        axiomsWithDup += axioms.size();
	        for(OWLLogicalAxiom ax : axioms) {
	        	if(ax instanceof OWLSubClassOfAxiom) {
	        		OWLSubClassOfAxiom subAxiom = (OWLSubClassOfAxiom)ax;
	        		OWLClassExpression subOCE = subAxiom.getSubClass();
	        		OWLClassExpression superOCE = subAxiom.getSuperClass();
	        		if(subOCE instanceof OWLClass) {
	        			if(superOCE instanceof OWLClass) {
	        				subClassAxiomsWithDup++;
	        			}
	        			else if(superOCE instanceof OWLObjectSomeValuesFrom) {
	        				rsetAxiomsWithDup++;
	        			}
	        		}
	        	}
	        }
	        manager.removeOntology(ontology);
	        numfiles++;
		}
		System.out.println("Num files: " + numfiles);
		System.out.println("\t Logical Axioms: " + axiomsWithDup);
		System.out.println("\t Subclass Axioms: " + subClassAxiomsWithDup);
		System.out.println("\t R(r) axioms: " + rsetAxiomsWithDup);
		System.out.println("\t Remaining axioms (add them to after " +
				"classification axiom count): " + 
				(axiomsWithDup - 
						(subClassAxiomsWithDup + rsetAxiomsWithDup)));
		System.out.println("\t Classes: " + numClassesWithDup);
	}
	
	
	public void getAxiomCountAfterClassification() {
		PropertyFileHandler propertyFileHandler = 
				PropertyFileHandler.getInstance();
		HostInfo resultHostInfo = propertyFileHandler.getResultNode();
		Jedis jedis = new Jedis(resultHostInfo.getHost(), 
				resultHostInfo.getPort(), Constants.INFINITE_TIMEOUT);
		String script = 
				"local keys = redis.call('KEYS', '*') " +
				"local totalAxioms = 0 " +
				"for index, value in pairs(keys) do " +
					"local b = redis.call('TYPE', value) " +
					"if(b.ok == 'zset' ) then " +	//type returns a lua table
						"local n = redis.call('ZCARD', value) " +
						"totalAxioms = totalAxioms + n " +
					"end " +
				"end " +
				"return totalAxioms ";
		Long numSubClassAxioms = (Long) jedis.eval(script);
		int nodeCount = propertyFileHandler.getNodeCount();
		//deduct nodes since they are also of zset type
		System.out.println("Total subclass axioms: " + 
				(numSubClassAxioms-nodeCount));
		
		// All R(r) would be in T3-2. All other nodes holding R(r) would have
		// partial values.
		Set<String> type32Hosts = jedis.zrange(
				AxiomDistributionType.CR_TYPE3_2.toString(), 
				Constants.RANGE_BEGIN, Constants.RANGE_END);
		jedis.close();
		long totalRsetValues = 0;
		String countRValsScript = 
				"local keys = redis.call('ZRANGE', 'localkeys', 0, -1) " +
				"local rcount = 0 " +
				"for index, value in pairs(keys) do " +
					"local n = redis.call('ZCARD', value) " +
					"rcount = rcount + n " +
				"end " +
				"return rcount ";
		for(String host : type32Hosts) {
			String[] hostPort = host.split(":");
			Jedis type32Jedis = new Jedis(hostPort[0], 
					Integer.parseInt(hostPort[1]), Constants.INFINITE_TIMEOUT);
			type32Jedis.select(AxiomDB.ROLE_DB.getDBIndex());
			Long count = (Long) type32Jedis.eval(countRValsScript);
			totalRsetValues += count;
			type32Jedis.close();
		}
		System.out.println("Total R(r) values: " + totalRsetValues);
	}
	
	public static void main(String[] args) throws Exception {
		if(args.length != 1) {
			System.out.println("Path or directory of the ontology/ontologies " +
					"is required");
			System.exit(-1);
		}
		AxiomCounter axiomCounter = new AxiomCounter();
		System.out.println("\n------------Before classification ------------");
//		axiomCounter.getAxiomCountBeforeClassification(args[0]);
		axiomCounter.getAxiomCountBeforeClassificationWithoutDupCheck(args[0]);
		System.out.println("\n------------After classification ------------");
		axiomCounter.getAxiomCountAfterClassification();
	}
}
