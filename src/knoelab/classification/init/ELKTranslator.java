package knoelab.classification.init;

import java.io.File;
import java.util.HashSet;
import java.util.Set;

import org.semanticweb.owlapi.apibinding.OWLManager;
import org.semanticweb.owlapi.model.IRI;
import org.semanticweb.owlapi.model.OWLAxiom;
import org.semanticweb.owlapi.model.OWLClass;
import org.semanticweb.owlapi.model.OWLClassAssertionAxiom;
import org.semanticweb.owlapi.model.OWLClassExpression;
import org.semanticweb.owlapi.model.OWLDataFactory;
import org.semanticweb.owlapi.model.OWLIndividual;
import org.semanticweb.owlapi.model.OWLLogicalAxiom;
import org.semanticweb.owlapi.model.OWLObjectOneOf;
import org.semanticweb.owlapi.model.OWLObjectPropertyAssertionAxiom;
import org.semanticweb.owlapi.model.OWLObjectSomeValuesFrom;
import org.semanticweb.owlapi.model.OWLOntology;
import org.semanticweb.owlapi.model.OWLOntologyManager;
import org.semanticweb.owlapi.model.OWLSubClassOfAxiom;
import org.semanticweb.owlapi.util.SimpleIRIMapper;

/**
 * This class attempts to translate axioms in the given
 * ontology into a form that ELK reasoner understands.
 * For example, OWLObjectOneOf can written as a ClassAssertion.
 * 
 * @author Raghava
 */
public class ELKTranslator {

	private OWLOntologyManager manager;
	private int count = 0;
	
	public ELKTranslator() {
		try {
			manager = OWLManager.createOWLOntologyManager();
			addTripsBaseOntologies(manager);
		}catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public void translateAxioms(File ontoFile, String outputDir) throws Exception {
		IRI documentIRI = IRI.create(ontoFile);
		OWLDataFactory dataFactory = manager.getOWLDataFactory();
        OWLOntology ontology = 
        	manager.loadOntologyFromOntologyDocument(documentIRI);
        Set<OWLLogicalAxiom> axioms = ontology.getLogicalAxioms();       
        
        Set<OWLAxiom> axiomsToRemove = new HashSet<OWLAxiom>();
        Set<OWLAxiom> axiomsToAdd = new HashSet<OWLAxiom>();
        for(OWLLogicalAxiom ax : axioms) {
        	if(ax instanceof OWLSubClassOfAxiom) {
        		OWLSubClassOfAxiom sax = (OWLSubClassOfAxiom) ax;
        		OWLClassExpression subClass = sax.getSubClass();
        		OWLClassExpression supClass = sax.getSuperClass();
        		if(subClass instanceof OWLObjectOneOf) { 
        			if(supClass instanceof OWLClass) {
	        			Set<OWLIndividual> individuals = 
	        					((OWLObjectOneOf) subClass).getIndividuals();
	        			//there will only be one individual due to pre-processing
	        			OWLClassAssertionAxiom classAssertion = 
	        					dataFactory.getOWLClassAssertionAxiom(supClass, 
	        					individuals.iterator().next());
	        			axiomsToRemove.add(ax);
	        			axiomsToAdd.add(classAssertion);
        			}
        			else if(supClass instanceof OWLObjectSomeValuesFrom) {
        				OWLObjectSomeValuesFrom osv = 
        						(OWLObjectSomeValuesFrom) supClass;
        				OWLClassExpression oce = osv.getFiller();
        				if(oce instanceof OWLObjectOneOf) {
        					Set<OWLIndividual> subIndividuals = 
    	        					((OWLObjectOneOf) subClass).getIndividuals();
        					Set<OWLIndividual> supIndividuals = 
    	        					((OWLObjectOneOf) oce).getIndividuals();
        					//there will only be one individual due to pre-processing
        					if(subIndividuals.size() > 1 || supIndividuals.size() > 1)
        						throw new Exception("ObjectOneOf should have " +
        								"only 1 arg");
        					OWLObjectPropertyAssertionAxiom propAssertion = 
        							dataFactory.getOWLObjectPropertyAssertionAxiom(
        							osv.getProperty(), 
        							subIndividuals.iterator().next(), 
        							supIndividuals.iterator().next());
        					axiomsToRemove.add(ax);
    	        			axiomsToAdd.add(propAssertion);
        				}
        			}
        		}
        	}
        }
        System.out.println("Axioms to remove: " + axiomsToRemove.size());
        System.out.println("Axioms to add: " + axiomsToAdd.size());
        manager.removeAxioms(ontology, axiomsToRemove);
        manager.addAxioms(ontology, axiomsToAdd);
        File file = new File(outputDir + File.separator 
    	   			+ ontoFile.getName());
        manager.saveOntology(ontology, IRI.create(file));       
        manager.removeOntology(ontology);
        count++;
        System.out.println("Done with " + count + "\n");
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
		if(args.length != 2) {
			System.out.println("Provide input directory containing ontology " +
					"files and output directory");
			System.exit(-1);
		}
		ELKTranslator translator = new ELKTranslator();
		File input = new File(args[0]);
		if(input.isFile())
			translator.translateAxioms(input, args[1]);
		else if(input.isDirectory()) {
			File[] files = input.listFiles();
			for(File file : files)
				translator.translateAxioms(file, args[1]);
		}
		else
			throw new Exception("Unknown input type - " +
					"only file or directory allowed");
	}
}
