package knoelab.classification.init;

import java.io.File;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.semanticweb.owlapi.apibinding.OWLManager;
import org.semanticweb.owlapi.model.AxiomType;
import org.semanticweb.owlapi.model.IRI;
import org.semanticweb.owlapi.model.OWLClass;
import org.semanticweb.owlapi.model.OWLClassAssertionAxiom;
import org.semanticweb.owlapi.model.OWLClassExpression;
import org.semanticweb.owlapi.model.OWLDataFactory;
import org.semanticweb.owlapi.model.OWLIndividual;
import org.semanticweb.owlapi.model.OWLObjectPropertyAssertionAxiom;
import org.semanticweb.owlapi.model.OWLOntology;
import org.semanticweb.owlapi.model.OWLOntologyManager;
import org.semanticweb.owlapi.model.OWLSubClassOfAxiom;

/**
 * This class is used to convert axioms on individuals to axioms
 * involving concepts. Class assertions are converted to subclass
 * axioms and object property assertions are converted to axioms
 * of the form A < 3r.B 
 *  
 * @author Raghava
 *
 */
public class Ind2ClassConverter {

	private Map<OWLIndividual, OWLClassExpression> indClassMap; 
    private OWLDataFactory dataFactory;
    private String gensymClassPrefix = "http://knoelab.wright.edu/ontologies#ClassI";
    private long classCounter = 0;
	
	public Ind2ClassConverter(OWLDataFactory dataFactory) {
		indClassMap = new HashMap<OWLIndividual, OWLClassExpression>();
		this.dataFactory = dataFactory;
	}
	
	public Set<OWLSubClassOfAxiom> convertClassAssertions(
			Set<OWLClassAssertionAxiom> classAssertions) {
		Set<OWLSubClassOfAxiom> subClassAxioms = new HashSet<OWLSubClassOfAxiom>();
		for(OWLClassAssertionAxiom cassert : classAssertions) {
			OWLIndividual individual = cassert.getIndividual();
			OWLClassExpression ce = cassert.getClassExpression();
			OWLClassExpression subClassExpression;
			if(indClassMap.containsKey(individual))
				subClassExpression = indClassMap.get(individual);
			else {
				// generate a new class for this individual
				classCounter++;
				subClassExpression = dataFactory.getOWLClass(
						IRI.create(gensymClassPrefix + classCounter));
				indClassMap.put(individual, subClassExpression);
			}
			subClassAxioms.add(dataFactory.getOWLSubClassOfAxiom(subClassExpression, ce));
		}
		return subClassAxioms;
	}
	
	public Set<OWLSubClassOfAxiom> convertObjPropertyAssertions(
			Set<OWLObjectPropertyAssertionAxiom> objPropAssertions) 
			throws Exception {
		Set<OWLSubClassOfAxiom> subClassExistentialAxioms = 
			new HashSet<OWLSubClassOfAxiom>();
		for(OWLObjectPropertyAssertionAxiom propAssert : objPropAssertions) {
			OWLClassExpression subExpr = indClassMap.get(propAssert.getSubject());
			OWLClassExpression objExpr = indClassMap.get(propAssert.getObject());
			if(subExpr == null || objExpr == null)
				throw new Exception("One of subject: " + subExpr.toString() + 
						"  object: " + objExpr.toString() + " is null");
			subClassExistentialAxioms.add(
					dataFactory.getOWLSubClassOfAxiom(subExpr, 
							dataFactory.getOWLObjectSomeValuesFrom(
									propAssert.getProperty(), objExpr)));
		}
		return subClassExistentialAxioms;
	}
	
	public static void main(String[] args) throws Exception {
		if(args.length != 1) {
			System.out.println("Enter path to the Ontology");
			System.exit(-1);
		}
		OWLOntologyManager manager = OWLManager.createOWLOntologyManager();
        File owlFile = new File(args[0]);
        IRI documentIRI = IRI.create(owlFile);
        OWLOntology ontology = 
        	manager.loadOntologyFromOntologyDocument(documentIRI);
		Ind2ClassConverter converter = new Ind2ClassConverter(manager.getOWLDataFactory());
		Set<OWLSubClassOfAxiom> axioms = 
			converter.convertClassAssertions(
					ontology.getAxioms(AxiomType.CLASS_ASSERTION));
		int count = 0;
		for(OWLSubClassOfAxiom ax : axioms) {
			System.out.println(ax.toString());
			count++;
			if(count == 10)
				break;
		}
		axioms.clear();
		System.out.println();
		axioms = converter.convertObjPropertyAssertions(
				ontology.getAxioms(
						AxiomType.OBJECT_PROPERTY_ASSERTION));
		count = 0;
		for(OWLSubClassOfAxiom ax : axioms) {
			System.out.println(ax.toString());
			count++;
			if(count == 10)
				break;
		}
		System.out.println();
		Set<OWLClass> classes = ontology.getClassesInSignature();
		for(OWLClass cl : classes) 
			if(cl.isBottomEntity())
				System.out.println("Found Bottom: " + cl.toString());
	}
}
