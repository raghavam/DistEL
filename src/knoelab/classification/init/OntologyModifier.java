package knoelab.classification.init;

import java.io.File;
import java.util.HashSet;
import java.util.Set;

import org.semanticweb.owlapi.apibinding.OWLManager;
import org.semanticweb.owlapi.model.IRI;
import org.semanticweb.owlapi.model.OWLClassExpression;
import org.semanticweb.owlapi.model.OWLDataHasValue;
import org.semanticweb.owlapi.model.OWLDataSomeValuesFrom;
import org.semanticweb.owlapi.model.OWLDatatype;
import org.semanticweb.owlapi.model.OWLLogicalAxiom;
import org.semanticweb.owlapi.model.OWLObjectExactCardinality;
import org.semanticweb.owlapi.model.OWLOntology;
import org.semanticweb.owlapi.model.OWLOntologyManager;
import org.semanticweb.owlapi.model.OWLSubClassOfAxiom;

public class OntologyModifier {

	public void removeDatatypeAxioms(String ontDirPath) throws Exception {
		File ontDir = new File(ontDirPath);
		File[] owlFiles = ontDir.listFiles();
		OWLOntologyManager manager = OWLManager.createOWLOntologyManager();
		long count = 1;
		for(File owlFile : owlFiles) {
			System.out.println("Working on " + owlFile.getName() + "   " + count);
			count++;
			IRI documentIRI = IRI.create(owlFile);
	        OWLOntology ontology = manager.loadOntologyFromOntologyDocument(
	        							documentIRI);
	        Set<OWLLogicalAxiom> axioms = ontology.getLogicalAxioms();
	        Set<OWLLogicalAxiom> axiomsToBeRemoved = 
	        		new HashSet<OWLLogicalAxiom>();
	        Set<OWLDatatype> dataTypes = new HashSet<OWLDatatype>();
	        for(OWLLogicalAxiom axiom : axioms) {
	        	if(axiom instanceof OWLSubClassOfAxiom) {
	        		OWLClassExpression oce = 
	        				((OWLSubClassOfAxiom)axiom).getSuperClass();
	        		if(oce instanceof OWLDataHasValue) {
	        			OWLDatatype dataType = 
	        					((OWLDataHasValue)oce).getValue().getDatatype();
	        			axiomsToBeRemoved.add(axiom);
	        			dataTypes.add(dataType);
	        		}
	        	}
	        }
	        System.out.println("No. of logical axioms: " + axioms.size());
	        if(!axiomsToBeRemoved.isEmpty()) {
	        	manager.removeAxioms(ontology, axiomsToBeRemoved);
	        	manager.saveOntology(ontology);
	        	System.out.println("No. of data type axioms removed: " + 		
	        			axiomsToBeRemoved.size());
	        	System.out.println("Datatypes found: " + dataTypes);
	        	System.out.println("No. of logical axioms after removal: " + 
	        				ontology.getLogicalAxiomCount());
	        }
	        manager.removeOntology(ontology);
	        System.out.println();
		}
		System.out.println("Done \n");
	}
	
	public void removeDataSomeValuesFromAxioms(String ontDirPath) throws Exception {
		File ontDir = new File(ontDirPath);
		File[] owlFiles = ontDir.listFiles();
		OWLOntologyManager manager = OWLManager.createOWLOntologyManager();
        long count = 0;
        for(File owlFile : owlFiles) {
        	IRI documentIRI = IRI.create(owlFile);
	        OWLOntology ontology = manager.loadOntologyFromOntologyDocument(
	        							documentIRI);
	        Set<OWLLogicalAxiom> axioms = ontology.getLogicalAxioms();
	        Set<OWLLogicalAxiom> axiomsToBeRemoved = 
	        		new HashSet<OWLLogicalAxiom>();
	        for(OWLLogicalAxiom axiom : axioms) {
	        	if(axiom instanceof OWLSubClassOfAxiom) {
	        		OWLSubClassOfAxiom subClassAxiom = (OWLSubClassOfAxiom)axiom;
	        		OWLClassExpression oce1 = subClassAxiom.getSubClass();
	        		OWLClassExpression oce2 = subClassAxiom.getSuperClass();        		
	        		if(oce1 instanceof OWLDataSomeValuesFrom || 
        				oce2 instanceof OWLDataSomeValuesFrom || 
        				oce1 instanceof OWLObjectExactCardinality ||
        				oce2 instanceof OWLObjectExactCardinality) {
	        			axiomsToBeRemoved.add(axiom);
	        			count++;
	        		}
	        	}
	        }
	        if(!axiomsToBeRemoved.isEmpty()) {
	        	manager.removeAxioms(ontology, axiomsToBeRemoved);
	        	manager.saveOntology(ontology);
	        }
	        manager.removeOntology(ontology);
        }
        System.out.println("No of axioms removed: " + count);
	}
	
	public static void main(String[] args) throws Exception {
		if(args.length != 1) {
			System.out.println("Provide ontology directory path");
			System.exit(-1);
		}
//		new OntologyModifier().removeDatatypeAxioms(args[0]);
		new OntologyModifier().removeDataSomeValuesFromAxioms(args[0]);
	}

}
