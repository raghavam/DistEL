package knoelab.classification.init;

import java.io.File;
import java.util.HashSet;
import java.util.Set;

import org.semanticweb.owlapi.apibinding.OWLManager;
import org.semanticweb.owlapi.model.AxiomType;
import org.semanticweb.owlapi.model.IRI;
import org.semanticweb.owlapi.model.OWLAxiom;
import org.semanticweb.owlapi.model.OWLClass;
import org.semanticweb.owlapi.model.OWLClassAssertionAxiom;
import org.semanticweb.owlapi.model.OWLClassExpression;
import org.semanticweb.owlapi.model.OWLDataHasValue;
import org.semanticweb.owlapi.model.OWLDataSomeValuesFrom;
import org.semanticweb.owlapi.model.OWLDisjointClassesAxiom;
import org.semanticweb.owlapi.model.OWLEquivalentClassesAxiom;
import org.semanticweb.owlapi.model.OWLLogicalAxiom;
import org.semanticweb.owlapi.model.OWLObjectHasValue;
import org.semanticweb.owlapi.model.OWLObjectIntersectionOf;
import org.semanticweb.owlapi.model.OWLObjectOneOf;
import org.semanticweb.owlapi.model.OWLObjectPropertyAssertionAxiom;
import org.semanticweb.owlapi.model.OWLObjectPropertyDomainAxiom;
import org.semanticweb.owlapi.model.OWLObjectPropertyRangeAxiom;
import org.semanticweb.owlapi.model.OWLObjectSomeValuesFrom;
import org.semanticweb.owlapi.model.OWLOntology;
import org.semanticweb.owlapi.model.OWLOntologyManager;
import org.semanticweb.owlapi.model.OWLSubClassOfAxiom;
import org.semanticweb.owlapi.model.OWLSubObjectPropertyOfAxiom;
import org.semanticweb.owlapi.model.OWLSubPropertyChainOfAxiom;
import org.semanticweb.owlapi.model.OWLTransitiveObjectPropertyAxiom;


/**
 * This class checks the axioms of the given ontology
 * and removes the axioms which are not supported by our
 * system. Currently supports EL++ without data types.
 * 
 * @author Raghava
 */
public class ProfileChecker {

	private OWLOntologyManager manager;
	
	public ProfileChecker() {
		manager = OWLManager.createOWLOntologyManager();
	}
	
	public void checkProfile(File ontoFile) throws Exception {
		IRI documentIRI = IRI.create(ontoFile);
        OWLOntology ontology = 
        	manager.loadOntologyFromOntologyDocument(documentIRI);
        Set<OWLLogicalAxiom> axioms = ontology.getLogicalAxioms();
        System.out.println("Logical axioms: " + axioms.size());
        Set<String> removedTypes = new HashSet<String>();
        Set<OWLAxiom> axiomsToRemove = new HashSet<OWLAxiom>();
        for(OWLLogicalAxiom ax : axioms) {
        	if(ax instanceof OWLSubClassOfAxiom) {
        		OWLSubClassOfAxiom sax = (OWLSubClassOfAxiom) ax;
        		OWLClassExpression subClass = sax.getSubClass();
        		OWLClassExpression supClass = sax.getSuperClass();
        		if(!isAcceptableType(subClass)) {
        			removedTypes.add(subClass.getClass().getSimpleName());
        			axiomsToRemove.add(ax);
        		}
        		if(!isAcceptableType(supClass)) {
        			removedTypes.add(supClass.getClass().getSimpleName());
        			axiomsToRemove.add(ax);
        		}
        	}
        	else if(ax instanceof OWLSubObjectPropertyOfAxiom) 
        		continue;
        	else if(ax instanceof OWLSubPropertyChainOfAxiom)
        		continue;
        	else if(ax instanceof OWLObjectPropertyDomainAxiom)
        		continue;
        	else if(ax instanceof OWLObjectPropertyRangeAxiom) {
        		OWLClassExpression range = 
        				((OWLObjectPropertyRangeAxiom) ax).getRange();
        		if(range instanceof OWLClass)
        			continue;
        		else {
        			removedTypes.add(range.getClass().getSimpleName());
            		axiomsToRemove.add(ax);
        		}       			
        	}
//        	else if(ax instanceof OWLDataPropertyDomainAxiom) //ELK doesn't support this
//        		continue;
        	else if(ax instanceof OWLDisjointClassesAxiom)
        		continue;
        	else if (ax instanceof OWLTransitiveObjectPropertyAxiom)
        		continue;
        	else if (ax instanceof OWLEquivalentClassesAxiom)
        		continue;
//        	else if(ax instanceof OWLClassAssertionAxiom)
//        		continue;
        	else if(ax instanceof OWLObjectPropertyAssertionAxiom)
        		continue;
        	else {
        		removedTypes.add(ax.getClass().getSimpleName());
        		axiomsToRemove.add(ax);
        	}
        }
        if(!axiomsToRemove.isEmpty()) {
        	manager.removeAxioms(ontology, axiomsToRemove);
//            manager.saveOntology(ontology, documentIRI);
	        System.out.println("Axiom types removed are: ");
	        for(String s : removedTypes)
	        	System.out.println(s);
        }
        System.out.println("Number of axioms removed: " + axiomsToRemove.size());
	}
	
	private boolean isAcceptableType(OWLClassExpression oce) {
		boolean isAcceptableType = false;
		if(oce instanceof OWLClass)
			isAcceptableType = true;
//		else if(oce instanceof OWLObjectOneOf) {
//			if(((OWLObjectOneOf) oce).getIndividuals().size() > 1) 
//				isAcceptableType = false;
//			else
//				isAcceptableType = true;
//		}
		else if(oce instanceof OWLObjectSomeValuesFrom)
			isAcceptableType = true;
//		else if(oce instanceof OWLDataSomeValuesFrom)
//			isAcceptableType = true;
//		else if(oce instanceof OWLObjectHasValue)
//			isAcceptableType = true;
		else if(oce instanceof OWLObjectIntersectionOf)
			isAcceptableType = true;
		
		return isAcceptableType;
	}
	
	public void removeDataHasValueAxioms(File ontoFile) 
			throws Exception {
		IRI documentIRI = IRI.create(ontoFile);
        OWLOntology ontology = 
        	manager.loadOntologyFromOntologyDocument(documentIRI);
        Set<String> removedTypes = new HashSet<String>();
        Set<OWLAxiom> axiomsToRemove = new HashSet<OWLAxiom>();
        Set<OWLSubClassOfAxiom> axioms = ontology.getAxioms(AxiomType.SUBCLASS_OF);
        for(OWLSubClassOfAxiom ax : axioms) {
        	OWLClassExpression oce = ax.getSuperClass();
        	if(oce instanceof OWLDataHasValue) {
        		axiomsToRemove.add(ax);
        		removedTypes.add(oce.getClass().getSimpleName());
        	}
        }
        manager.removeAxioms(ontology, axiomsToRemove);
        manager.saveOntology(ontology, documentIRI);
        System.out.println("Axiom types removed are: ");
        for(String s : removedTypes)
        	System.out.println(s);
        System.out.println("Number of axioms removed: " + axiomsToRemove.size());
        manager.removeOntology(ontology);
	}
	
	public static void main(String[] args) throws Exception {
		if(args.length != 1) {
			System.out.println("Provide input file or directory " +
					"containing ontology");
			System.exit(-1);
		}
		ProfileChecker profileChecker = new ProfileChecker();
		File input = new File(args[0]);
		if(input.isFile())
			profileChecker.checkProfile(input);
//			profileChecker.removeDataHasValueAxioms(input);
		else if(input.isDirectory()) {
			File[] files = input.listFiles();
			for(File file : files)
				profileChecker.checkProfile(file);
//				profileChecker.removeDataHasValueAxioms(file);
		}
		else
			throw new Exception("Unknown input type - " +
					"only file or directory allowed");
	}
}
