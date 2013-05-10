package knoelab.classification.misc;

import java.io.File;
import java.util.Set;

import org.semanticweb.owlapi.apibinding.OWLManager;
import org.semanticweb.owlapi.model.AxiomType;
import org.semanticweb.owlapi.model.IRI;
import org.semanticweb.owlapi.model.OWLAxiom;
import org.semanticweb.owlapi.model.OWLClass;
import org.semanticweb.owlapi.model.OWLClassAssertionAxiom;
import org.semanticweb.owlapi.model.OWLClassExpression;
import org.semanticweb.owlapi.model.OWLDataPropertyAssertionAxiom;
import org.semanticweb.owlapi.model.OWLNamedIndividual;
import org.semanticweb.owlapi.model.OWLObjectProperty;
import org.semanticweb.owlapi.model.OWLObjectPropertyAssertionAxiom;
import org.semanticweb.owlapi.model.OWLObjectSomeValuesFrom;
import org.semanticweb.owlapi.model.OWLOntology;
import org.semanticweb.owlapi.model.OWLOntologyManager;
import org.semanticweb.owlapi.model.OWLSubClassOfAxiom;


/**
 * This class prints the ontology stats
 * @author Raghava
 *
 */
public class OntologyStats {
	
	public void printOntologyStats(String ontoFile) throws Exception {
		OWLOntologyManager manager = OWLManager.createOWLOntologyManager();
        File owlFile = new File(ontoFile);
        IRI documentIRI = IRI.create(owlFile);
        OWLOntology ontology = 
        	manager.loadOntologyFromOntologyDocument(documentIRI);
        System.out.println("LogicalAxioms: " + ontology.getLogicalAxiomCount());
        System.out.println("No. of Classes: " + 
        		ontology.getClassesInSignature().size());
        System.out.println("No of Roles: " + 
        		ontology.getObjectPropertiesInSignature().size());
        
        Set<OWLSubClassOfAxiom> subClassAxioms = 
        	ontology.getAxioms(AxiomType.SUBCLASS_OF);
        System.out.println("No of sub class axioms: " + subClassAxioms.size());
//        Set<OWLAxiom> aboxAxioms = ontology.getABoxAxioms(false);
//        System.out.println("ABox axioms: " + aboxAxioms.size());
        Set<OWLNamedIndividual> namedIndividuals = ontology.getIndividualsInSignature();
        System.out.println("Named Individuals: " + namedIndividuals.size());
        Set<OWLClassAssertionAxiom> classAssertionAxioms = 
        	ontology.getAxioms(AxiomType.CLASS_ASSERTION);
        System.out.println("Class Assertion Axioms: " + classAssertionAxioms.size());
        Set<OWLObjectPropertyAssertionAxiom> objPropAssertionAxioms = 
        	ontology.getAxioms(AxiomType.OBJECT_PROPERTY_ASSERTION);
        System.out.println("Obj Property Assertion Axioms: " + 
        		objPropAssertionAxioms.size());
        Set<OWLDataPropertyAssertionAxiom> dataPropAssertionAxioms = 
        	ontology.getAxioms(AxiomType.DATA_PROPERTY_ASSERTION);
        System.out.println("Data Assertion Axioms: " + 
        		dataPropAssertionAxioms.size());
        
        int cnt1 = 0;
        int cnt2 = 0;
        for(OWLSubClassOfAxiom ax : subClassAxioms) {
        	OWLClassExpression s1 = ax.getSubClass();
        	OWLClassExpression s2 = ax.getSuperClass();
        	if(s1 instanceof OWLClass) {
        		if(s2 instanceof OWLClass)
        			cnt1++;
        		else if(s2 instanceof OWLObjectSomeValuesFrom)
        			cnt2++;
        	}
        }
        System.out.println("A < B: " + cnt1);
        System.out.println("A < 3r.B: " + cnt2);
        
        manager.removeOntology(ontology);
	}

	public static void main(String[] args) throws Exception {
		if(args.length != 1) {
			System.out.println("Enter path to the Ontology");
			System.exit(-1);
		}
		new OntologyStats().printOntologyStats(args[0]);
	}

}
