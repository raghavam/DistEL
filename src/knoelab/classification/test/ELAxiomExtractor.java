package knoelab.classification.test;

import java.io.File;
import java.util.HashSet;
import java.util.Set;

import org.semanticweb.owlapi.apibinding.OWLManager;
import org.semanticweb.owlapi.model.IRI;
import org.semanticweb.owlapi.model.OWLClass;
import org.semanticweb.owlapi.model.OWLClassExpression;
import org.semanticweb.owlapi.model.OWLDisjointClassesAxiom;
import org.semanticweb.owlapi.model.OWLEquivalentClassesAxiom;
import org.semanticweb.owlapi.model.OWLLogicalAxiom;
import org.semanticweb.owlapi.model.OWLObjectIntersectionOf;
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
 * Extracts only EL+ axioms from the given ontology. Rest are
 * ignored. This is useful for testing ontologies with other
 * reasoners.
 * 
 * @author Raghava
 */
public class ELAxiomExtractor {

	private OWLOntologyManager manager;
	private Set<String> removedTypes;
	
	public ELAxiomExtractor() {
		manager = OWLManager.createOWLOntologyManager();
		removedTypes = new HashSet<String>();
	}
	
	public void extractELAxioms(File baseOntFile, File dir) 
			throws Exception {
		IRI documentIRI = IRI.create(baseOntFile);
        OWLOntology baseOntology = 
        	manager.loadOntologyFromOntologyDocument(documentIRI);
        OWLOntologyManager manager2 = OWLManager.createOWLOntologyManager();
        OWLOntology mergedOntology = manager2.createOntology(
        		IRI.create(new File("all-traffic-merged.owl")));
        Set<OWLLogicalAxiom> axioms = baseOntology.getLogicalAxioms(); 
        for(OWLLogicalAxiom ax : axioms)
        	if(checkAxiom(ax))
        		manager2.addAxiom(mergedOntology, ax);
        manager.removeOntology(baseOntology);
        File[] allFiles = dir.listFiles();
        int count = 0;
        for(File owlFile : allFiles) {
        	OWLOntology ontology = manager.loadOntologyFromOntologyDocument(
        			IRI.create(owlFile));
        	Set<OWLLogicalAxiom> logicalAxioms = ontology.getLogicalAxioms();
        	for(OWLLogicalAxiom ax : logicalAxioms)
        		if(checkAxiom(ax))
        			manager2.addAxiom(mergedOntology, ax);
        	manager.removeOntology(ontology);
        	count++;
        	System.out.println("Done with " + count);
        }
        System.out.println("Total axioms: " + mergedOntology.getLogicalAxiomCount());
        manager2.saveOntology(mergedOntology);
        System.out.println("Removed types: ");
        for(String s : removedTypes)
        	System.out.println(s);
	}
	
	public boolean checkAxiom(OWLLogicalAxiom ax) {
		boolean isAxiomInEL = true;
		if(ax instanceof OWLSubClassOfAxiom) {
    		OWLSubClassOfAxiom sax = (OWLSubClassOfAxiom) ax;
    		OWLClassExpression subClass = sax.getSubClass();
    		OWLClassExpression supClass = sax.getSuperClass();
    		if(isAcceptableType(subClass)) 
    			isAxiomInEL = true;
    		else {
    			removedTypes.add(subClass.getClass().getSimpleName());
    			isAxiomInEL = false;
    		}
    		if(isAcceptableType(supClass)) 
    			isAxiomInEL = true;
    		else {
    			removedTypes.add(supClass.getClass().getSimpleName());
    			isAxiomInEL = false;
    		}
    	}
    	else if(ax instanceof OWLSubObjectPropertyOfAxiom) 
    		isAxiomInEL = true;
    	else if(ax instanceof OWLSubPropertyChainOfAxiom)
    		isAxiomInEL = true;
    	else if(ax instanceof OWLObjectPropertyDomainAxiom)
    		isAxiomInEL = true;
    	else if(ax instanceof OWLObjectPropertyRangeAxiom) {
    		OWLClassExpression range = 
    				((OWLObjectPropertyRangeAxiom) ax).getRange();
    		if(range instanceof OWLClass)
    			isAxiomInEL = true;
    		else {
    			removedTypes.add(range.getClass().getSimpleName());
    			isAxiomInEL = false;
    		}       			
    	}
    	else if(ax instanceof OWLDisjointClassesAxiom)
    		isAxiomInEL = true;
    	else if (ax instanceof OWLTransitiveObjectPropertyAxiom)
    		isAxiomInEL = true;
    	else if (ax instanceof OWLEquivalentClassesAxiom)
    		isAxiomInEL = true;
    	else if(ax instanceof OWLObjectPropertyAssertionAxiom)
    		isAxiomInEL = true;
    	else {
    		removedTypes.add(ax.getClass().getSimpleName());
    		isAxiomInEL = false;
    	}
		return isAxiomInEL;
	}
	
	private boolean isAcceptableType(OWLClassExpression oce) {
		boolean isAcceptableType = false;
		if(oce instanceof OWLClass)
			isAcceptableType = true;
		else if(oce instanceof OWLObjectSomeValuesFrom)
			isAcceptableType = true;
		else if(oce instanceof OWLObjectIntersectionOf)
			isAcceptableType = true;
		
		return isAcceptableType;
	}
	
	public static void main(String[] args) throws Exception {
		if(args.length != 2) {
			System.out.println("Provide base ontology path and directory " +
					"containing traffic data");
			System.exit(-1);
		}
		new ELAxiomExtractor().extractELAxioms(new File(args[0]), 
				new File(args[1]));
	}
}
