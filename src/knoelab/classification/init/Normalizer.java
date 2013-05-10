/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package knoelab.classification.init;

import java.io.File;
import java.util.ArrayList;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.Stack;
import java.util.HashSet;

import knoelab.classification.misc.Util;

import org.semanticweb.owlapi.apibinding.OWLManager;
import org.semanticweb.owlapi.model.AxiomType;
import org.semanticweb.owlapi.model.IRI;
import org.semanticweb.owlapi.model.OWLAxiom;
import org.semanticweb.owlapi.model.OWLClass;
import org.semanticweb.owlapi.model.OWLClassAssertionAxiom;
import org.semanticweb.owlapi.model.OWLClassExpression;
import org.semanticweb.owlapi.model.OWLDataFactory;
import org.semanticweb.owlapi.model.OWLEquivalentClassesAxiom;
import org.semanticweb.owlapi.model.OWLLogicalAxiom;
import org.semanticweb.owlapi.model.OWLObjectIntersectionOf;
import org.semanticweb.owlapi.model.OWLObjectProperty;
import org.semanticweb.owlapi.model.OWLObjectPropertyAssertionAxiom;
import org.semanticweb.owlapi.model.OWLObjectPropertyDomainAxiom;
import org.semanticweb.owlapi.model.OWLObjectPropertyExpression;
import org.semanticweb.owlapi.model.OWLObjectSomeValuesFrom;
import org.semanticweb.owlapi.model.OWLOntology;
import org.semanticweb.owlapi.model.OWLOntologyCreationException;
import org.semanticweb.owlapi.model.OWLOntologyManager;
import org.semanticweb.owlapi.model.OWLSubClassOfAxiom;
import org.semanticweb.owlapi.model.OWLSubObjectPropertyOfAxiom;
import org.semanticweb.owlapi.model.OWLSubPropertyChainOfAxiom;
import org.semanticweb.owlapi.model.OWLTransitiveObjectPropertyAxiom;

/**
 * A class for putting EL axioms into normal form. applies the rules NF1-NF7 exhaustively.
 * For the rules, see the 2005 "Pushing the EL envelope" technical report.
 * Input/Output is a stack of axioms.
 * 
 * @author Frederick Maier, Raghava
 */
public class Normalizer {

    private OWLOntologyManager manager;
    private OWLDataFactory datafactory;
    private long classCounter = 0;
    private long propertyCounter = 0;
    private String gensymClassPrefix = "http://knoelab.wright.edu/ontologies#Class";
    private String gensymPropertyPrefix = "http://knoelab.wright.edu/ontologies#Property";
    private OWLOntology inputOntology;
    private OWLOntology outputOntology;
    
    // This hashtable is used to keep track of the complex concepts that
	// have been replaced with its equivalent simple new concepts
	private HashMap<OWLClassExpression, OWLClassExpression> exprConceptMap;
	private int hits = 0;

    /**
     * Creates a Normalizer, using the specified ontology manager and ontology.
     * @param m The manager used. It is needed to manipulate OWL axioms, classes, and properties.
     */
    public Normalizer(OWLOntologyManager m, OWLOntology inOntology) throws OWLOntologyCreationException {
        manager = m;
        datafactory = m.getOWLDataFactory();
        inputOntology = inOntology;
        outputOntology = m.createOntology();
        exprConceptMap = new HashMap<OWLClassExpression, OWLClassExpression>();
    }

    /**
     * Normalizes an EL axiom.  See stack based version for further details.
     * @param axiom The axiom to process.
     * @return The processed axiom.
     * @throws Exception 
     */
    public OWLOntology Normalize() throws Exception {
    	
    	// Remove ObjectPropertyRange axioms. They are not part of EL+
    	manager.removeAxioms(inputOntology, 
    			inputOntology.getAxioms(AxiomType.OBJECT_PROPERTY_RANGE));
    	
    	// Remove class assertions and replace them with subclass axioms. Also
    	// remove object property assertions and replace them with axioms of 
    	// the form A < 3r.B
    	Set<OWLClassAssertionAxiom> classAssertions = 
    		inputOntology.getAxioms(AxiomType.CLASS_ASSERTION);
    	Set<OWLObjectPropertyAssertionAxiom> objPropAssertions = 
    		inputOntology.getAxioms(AxiomType.OBJECT_PROPERTY_ASSERTION);
    	if(!classAssertions.isEmpty() && !objPropAssertions.isEmpty()) {
	    	Ind2ClassConverter converter = new Ind2ClassConverter(datafactory);
	    	Set<OWLSubClassOfAxiom> subClassAxioms = 
	    		converter.convertClassAssertions(classAssertions);
	    	Set<OWLSubClassOfAxiom> subClassExistentialAxioms = 
	    		converter.convertObjPropertyAssertions(objPropAssertions);
	    	manager.removeAxioms(inputOntology, classAssertions);
	    	manager.removeAxioms(inputOntology, objPropAssertions);
	    	manager.addAxioms(inputOntology, subClassAxioms);
	    	manager.addAxioms(inputOntology, subClassExistentialAxioms);
    	}
    	
    	Set<OWLLogicalAxiom> logicalAxioms = inputOntology.getLogicalAxioms();
        for (OWLAxiom ax : logicalAxioms) {
            Stack<OWLAxiom> stack = Normalize(ax);
            for (OWLAxiom el : stack) {
                manager.addAxiom(outputOntology, el);
            }
        }
        Set<OWLAxiom> ontologyAxioms = inputOntology.getAxioms();
        
        // add all remaining axioms -- no need to normalize them
        ontologyAxioms.removeAll(logicalAxioms);
        manager.addAxioms(outputOntology, ontologyAxioms);
        System.out.println("No of hits (reused concepts): " + hits);
        exprConceptMap.clear();
        return outputOntology;
    }

    /**
     * Normalizes a set of EL axioms.  Uses a stack to process each axiom.
     * The algorithm works in two phases:
     * 1) apply rules NF1-NF4 exhaustively, which generates a stack of intermediate axioms.
     * 2) Apply rules NF5-NF7 (and NF8) exhaustively, which generates the final stack of axioms.
     * NF8 is an additional rule to split, e.g., "A^B^C < D" into "A ^ B < CNew", "CNew ^ C < D".
     * These are then returned.
     * @param axioms The stack of axioms to process.
     * @return The processed set of axioms.
     * @throws Exception 
     */
    private Stack<OWLAxiom> Normalize(Stack<OWLAxiom> axiomStack) throws Exception {

        Stack<OWLAxiom> tempStack = new Stack<OWLAxiom>();

        //Phase 1: apply NF1-NF4 exhaustively, generating intermediate axioms.
        while (!axiomStack.isEmpty()) {
            OWLAxiom ax = axiomStack.pop();
            Set<? extends OWLAxiom> tempResults = NormalizePhase1(ax);
            if (tempResults == null) {
                tempStack.push(ax);
            } 
            else {
                for (OWLAxiom tempax : tempResults) {
                    axiomStack.push(tempax);
                }
            }
        }
        //Phase 2: apply NF5-NF7 exhaustively, generating final axioms.
        while (!tempStack.isEmpty()) {
            OWLAxiom ax = tempStack.pop();
            Set<OWLAxiom> tempResults = NormalizePhase2(ax);
            if (tempResults == null) {
                axiomStack.push(ax);
            } else {
                for (OWLAxiom tempax : tempResults) {
                    tempStack.push(tempax);
                }
            }
        }

        return axiomStack;
    }

    /**
     * Normalizes an EL axiom.  See stack based version for further details.
     * @param axiom The axiom to process.
     * @return The processed axiom.
     * @throws Exception 
     */
    private Stack<OWLAxiom> Normalize(OWLAxiom ax) throws Exception {
        Stack<OWLAxiom> axiomStack = new Stack<OWLAxiom>();
        axiomStack.push(ax);
        return Normalize(axiomStack);
    }

    /**
     * Applies one of rules NF1-NF4 to an axiom, returning an intermediate set of axioms.
     * If no rule applies, then null is returned, indicating the axiom cannot be further reduced.
     * @param ax The axiom to process.
     * @return  A set of axioms; the result of applying one of NF1-NF4.
     * @throws Exception 
     */
    private Set<? extends OWLAxiom> NormalizePhase1(OWLAxiom ax) throws Exception {
        if (ax instanceof OWLSubPropertyChainOfAxiom) {
            return NormalizePhase1((OWLSubPropertyChainOfAxiom) ax);
        } 
        else if (ax instanceof OWLSubClassOfAxiom) {
            return NormalizePhase1((OWLSubClassOfAxiom) ax);
        } 
        else if (ax instanceof OWLEquivalentClassesAxiom) {
        	return ((OWLEquivalentClassesAxiom) ax).asOWLSubClassOfAxioms();
//            return NormalizePhase1((OWLEquivalentClassesAxiom) ax);
        }
        else if (ax instanceof OWLTransitiveObjectPropertyAxiom) {
			// convert to property chain axiom
			OWLTransitiveObjectPropertyAxiom transitivePropertyAxiom = (OWLTransitiveObjectPropertyAxiom) ax;
			List<OWLObjectPropertyExpression> subPropertyList = new ArrayList<OWLObjectPropertyExpression>();
			OWLObjectPropertyExpression ope = transitivePropertyAxiom.getProperty();
			subPropertyList.add(ope);
			subPropertyList.add(ope);
			OWLSubPropertyChainOfAxiom propertyChainAxiom = datafactory.getOWLSubPropertyChainOfAxiom(subPropertyList,
			                ope);

        	Set<OWLAxiom> propertyChainAxiomSet = new HashSet<OWLAxiom>();
        	propertyChainAxiomSet.add(propertyChainAxiom);
			return propertyChainAxiomSet;
		}
        else if(ax instanceof OWLObjectPropertyDomainAxiom) {
        	// convert this into the form 3r.T < C i.e. C is the domain of relation r
        	OWLObjectPropertyDomainAxiom domainAxiom = (OWLObjectPropertyDomainAxiom) ax;
        	OWLClassExpression domainClassExpression = datafactory.getOWLObjectSomeValuesFrom(
        				domainAxiom.getProperty(), datafactory.getOWLThing());
        	Set<OWLAxiom> convertedDomainAxiom = new HashSet<OWLAxiom>();
        	convertedDomainAxiom.add(datafactory.getOWLSubClassOfAxiom(domainClassExpression, domainAxiom.getDomain()));
        	return convertedDomainAxiom;
        }
        else if(ax instanceof OWLSubObjectPropertyOfAxiom)
        	return null;
        else {
        	throw new Exception("Unexpected axiom type. Axiom: " + ax + " type: " + ax.getAxiomType());
        }
    }

    /**
     * Checks to see if NF1 applies to the given axiom, and if so, applies the rule.
     * @param ax The axiom to translate.
     * @return  the axioms resulting from applying NF1, or null if the rule doesn't apply.
     */
    private Set<OWLAxiom> NormalizePhase1(OWLSubPropertyChainOfAxiom ax) {
        if (NF1Check(ax)) {
            return NF1(ax);
        } else {
            return null;
        }
    }

    /**
     * Checks to see if one of NF2-NF4 applies to the given axiom, and if so, applies the rule.
     * @param ax The axiom to translate.
     * @return  the axioms resulting from applying the rule, or null if no rule applies.
     */
    private Set<OWLAxiom> NormalizePhase1(OWLSubClassOfAxiom ax) {


        if (NF2Check(ax)) {
            //System.out.println("NF2: " + ax);
            return NF2(ax);
        } else if (NF3Check(ax)) {
            //System.out.println("NF3: " + ax);
            return NF3(ax);
        } else if (NF4Check(ax)) {
            //System.out.println("NF4: " + ax);
            return NF4(ax);
        } else {
            return null;
        }
    }

    /**
     * Converts an equivalence axiom to  separate inclusion axioms. These
     * are returned as a set.
     * @param ax The OWLEquivalentClassesAxiom axiom to translate.
     * @return  A pair of inclusion axioms, as a set.
     */
/*    
    private Set<OWLAxiom> NormalizePhase1(OWLEquivalentClassesAxiom ax) {

        Set<OWLClassExpression> args = ax.getClassExpressions();
        Set<OWLAxiom> results = new HashSet<OWLAxiom>();
        for (OWLClassExpression c : args) {
            for (OWLClassExpression d : args) {
                if (c != d) {
                    results.add(datafactory.getOWLSubClassOfAxiom(c, d));
                }
            }
        }
        return results;

    }
*/    

    /**
     * Applies one of rules NF5-NF7 to an axiom, returning an set of axioms.
     * If no rule applies, then null is returned, indicating the axiom cannot be further reduced.
     * @param ax The axiom to process.
     * @return  A set of axioms; the result of applying one of NF5-NF7.
     */
    private Set<OWLAxiom> NormalizePhase2(OWLAxiom ax) {
        if (!(ax instanceof OWLSubClassOfAxiom)) {
            return null;
        } else if (NF5Check((OWLSubClassOfAxiom) ax)) {
            return NF5((OWLSubClassOfAxiom) ax);
        } else if (NF6Check((OWLSubClassOfAxiom) ax)) {
            return NF6((OWLSubClassOfAxiom) ax);
        } else if (NF7Check((OWLSubClassOfAxiom) ax)) {
            return NF7((OWLSubClassOfAxiom) ax);
        } //else if (NF8Check((OWLSubClassOfAxiom) ax)) {
        //return NF8((OWLSubClassOfAxiom) ax);    }
        else {
            return null;
        }
    }

    /**
     * Check to see if NF1 applies.
     * @param ax The axiom to check.
     * @return True if NF1 applies, and false otherwise.
     */
    private boolean NF1Check(OWLSubPropertyChainOfAxiom ax) {
        if (ax.getPropertyChain().size() > 2) {
            return true;
        } else {
            return false;
        }
    }

    /**
     * Check to see if NF2 applies.
     * Modification: NF2 is modified to take care of n-ary 
     * conjuction preservation (part of CEL optimization)
     * 
     * @param ax The axiom to check.
     * @return True if NF2 applies, and false otherwise.
     */
    private boolean NF2Check(OWLSubClassOfAxiom ax) {
        OWLClassExpression sub = ax.getSubClass();
        if (sub instanceof OWLObjectIntersectionOf) 
            return true;
        return false;
    }

    /**
     * Check to see if NF3 applies.
     * @param ax The axiom to check.
     * @return True if NF3 applies, and false otherwise.
     */
    private boolean NF3Check(OWLSubClassOfAxiom ax) {
        OWLClassExpression oce = ax.getSubClass();
        if (oce instanceof OWLObjectSomeValuesFrom) {
            OWLClassExpression filler = ((OWLObjectSomeValuesFrom) oce).getFiller();
            if (!isBasic(filler)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Check to see if NF4 applies.
     * @param ax The axiom to check.
     * @return True if NF4 applies, and false otherwise.
     */
    private boolean NF4Check(OWLSubClassOfAxiom ax) {
        OWLClassExpression oce = ax.getSubClass();
        if (oce.isOWLNothing()) {
            return true;
        }
        return false;
    }

    /**
     * Check to see if NF5 applies.
     * @param ax The axiom to check.
     * @return True if NF5 applies, and false otherwise.
     */
    private boolean NF5Check(OWLSubClassOfAxiom ax) {
        OWLClassExpression sub = ax.getSubClass();
        OWLClassExpression sup = ax.getSuperClass();

        if (!isBasic(sub) && !isBasic(sup)) {
            return true;
        }

        return false;
    }

    /**
     * Check to see if NF6 applies.
     * @param ax The axiom to check.
     * @return True if NF6 applies, and false otherwise.
     */
    private boolean NF6Check(OWLSubClassOfAxiom ax) {
        OWLClassExpression sup = ax.getSuperClass();
        OWLClassExpression sub = ax.getSubClass();
        if (sub instanceof OWLClass && sup instanceof OWLObjectSomeValuesFrom) {
            OWLClassExpression filler = ((OWLObjectSomeValuesFrom) sup).getFiller();
            if (!isBasic(filler)) {
                return true;
            } else {
                return false;
            }
        }
        return false;
    }

    /**
     * Check to see if NF7 applies.
     * @param ax The axiom to check.
     * @return True if NF7 applies, and false otherwise.
     */
    private boolean NF7Check(OWLSubClassOfAxiom ax) {
        OWLClassExpression sup = ax.getSuperClass();
        OWLClassExpression sub = ax.getSubClass();
        if (sub instanceof OWLClass && sup instanceof OWLObjectIntersectionOf) {
            return true;
        }
        return false;
    }

    /**
     * Apply NF1, returning a set of axioms.
     * @param ax The axiom to process.
     * @return The result of applying NF1.
     */
    private Set<OWLAxiom> NF1(OWLSubPropertyChainOfAxiom ax) {
        List<OWLObjectPropertyExpression> props = ax.getPropertyChain();
        Set<OWLAxiom> results = new HashSet<OWLAxiom>();
        if (props.size() == 1) {
            results.add(datafactory.getOWLSubObjectPropertyOfAxiom(props.get(0), ax.getSuperProperty()));
        } else {
            List<OWLObjectPropertyExpression> props2 = new ArrayList<OWLObjectPropertyExpression>();
            for (int i = 0; i < props.size() - 1; i++) {
                props2.add(props.get(i));
            }
            OWLObjectPropertyExpression u = gensymProperty();
            List<OWLObjectPropertyExpression> props3 = new ArrayList<OWLObjectPropertyExpression>();
            props3.add(u);
            props3.add(props.get(props.size() - 1));
            results.add(datafactory.getOWLSubPropertyChainOfAxiom(props2, u));
            results.add(datafactory.getOWLSubPropertyChainOfAxiom(props3, ax.getSuperProperty()));
        }
        return results;
    }

    /**
     * Apply NF2, returning a set of axioms.
     * Modification: NF2 is modified to take care of n-ary 
     * conjuction preservation (part of CEL optimization)
     * 
     * @param ax The axiom to process.
     * @return The result of applying NF2.
     */
    private Set<OWLAxiom> NF2(OWLSubClassOfAxiom ax) {
    	
    	OWLClassExpression superClass = ax.getSuperClass();
    	OWLObjectIntersectionOf intersectionAxiom = (OWLObjectIntersectionOf) ax.getSubClass();
    	Set<OWLClassExpression> operands = new HashSet<OWLClassExpression>(intersectionAxiom.getOperands());
    	Iterator<OWLClassExpression> operandsIt = operands.iterator();
    	Set<OWLClassExpression> normalizedOperands = new HashSet<OWLClassExpression>();
    	Set<OWLAxiom> normalizedAxioms = new HashSet<OWLAxiom>();
    	
    	while(operandsIt.hasNext()) {
    		OWLClassExpression op = operandsIt.next();
    		if(!isBasic(op)) {
    			// create a new class and replace this with a simple class
    			OWLClassExpression newClass = checkAndCreateConcept(op);
    			operandsIt.remove();
    			normalizedAxioms.add(datafactory.getOWLSubClassOfAxiom(op, newClass));
    			normalizedOperands.add(newClass);
    		}
    	}
    	if(!normalizedOperands.isEmpty()) {
    		operands.addAll(normalizedOperands);
    		normalizedAxioms.add(datafactory.getOWLSubClassOfAxiom(
    				datafactory.getOWLObjectIntersectionOf(operands), superClass));
    		return normalizedAxioms;
    	}
    	else 
    		return null;
    }

    /**
     * Apply NF3, returning a set of axioms.
     * @param ax The axiom to process.
     * @return The result of applying NF3.
     */
    private Set<OWLAxiom> NF3(OWLSubClassOfAxiom ax) {

        OWLClassExpression d = ax.getSuperClass();
        OWLObjectSomeValuesFrom sub = (OWLObjectSomeValuesFrom) ax.getSubClass();
        OWLClassExpression c = sub.getFiller();
        OWLClassExpression a = checkAndCreateConcept(c);
        Set<OWLAxiom> results = new HashSet<OWLAxiom>();
        results.add(datafactory.getOWLSubClassOfAxiom(c, a));
        results.add(datafactory.getOWLSubClassOfAxiom(
                datafactory.getOWLObjectSomeValuesFrom(sub.getProperty(), a),
                d));
        return results;
    }

    /**
     * Apply NF4, returning a set of axioms.
     * @param ax The axiom to process.
     * @return The result of applying NF4.
     */
    private Set<OWLAxiom> NF4(OWLSubClassOfAxiom ax) {
        return new HashSet<OWLAxiom>();
    }

    /**
     * Apply NF5, returning a set of axioms.
     * @param ax The axiom to process.
     * @return The result of applying NF5.
     */
    private Set<OWLAxiom> NF5(OWLSubClassOfAxiom ax) {
        OWLClassExpression c = ax.getSubClass();
        OWLClassExpression d = ax.getSuperClass();
        OWLClassExpression a = checkAndCreateConcept(c);

        Set<OWLAxiom> results = new HashSet<OWLAxiom>();
        results.add(datafactory.getOWLSubClassOfAxiom(c, a));
        results.add(datafactory.getOWLSubClassOfAxiom(a, d));
        return results;
    }

    /**
     * Apply NF6, returning a set of axioms.
     * @param ax The axiom to process.
     * @return The result of applying NF6.
     */
    private Set<OWLAxiom> NF6(OWLSubClassOfAxiom ax) {
        OWLObjectSomeValuesFrom sup = (OWLObjectSomeValuesFrom) ax.getSuperClass();

        OWLClassExpression b = ax.getSubClass();
        OWLClassExpression c = sup.getFiller();
        OWLClassExpression a = checkAndCreateConcept(c);

        Set<OWLAxiom> results = new HashSet<OWLAxiom>();
        results.add(datafactory.getOWLSubClassOfAxiom(b, datafactory.getOWLObjectSomeValuesFrom(sup.getProperty(), a)));
        results.add(datafactory.getOWLSubClassOfAxiom(a, c));
        return results;
    }

    /**
     * Apply NF7, returning a set of axioms.
     * @param ax The axiom to process.
     * @return The result of applying NF7.
     */
    private Set<OWLAxiom> NF7(OWLSubClassOfAxiom ax) {
        OWLObjectIntersectionOf inter = (OWLObjectIntersectionOf) ax.getSuperClass();
        OWLClassExpression b = ax.getSubClass();
        List<OWLClassExpression> args = inter.getOperandsAsList();
        Set<OWLAxiom> results = new HashSet<OWLAxiom>();
        for (OWLClassExpression c : args) {
            results.add(datafactory.getOWLSubClassOfAxiom(b, c));
        }
        return results;
    }

    /**
     * Tests whether the class expression is Thing, Nothing, or a class name (i.e., not anoymous).
     * @param oce The class expression to process.
     * @return True, if it is Thing, Nothing, or a class name, and false otherwise.
     */
    private boolean isBasic(OWLClassExpression oce) {
        if (oce.isOWLNothing() || oce.isOWLThing() || !oce.isAnonymous()) {
            return true;
        } else {
            return false;
        }
    }

    /**
     * Creates an OWL named class. the name will be of the form ClassPrefix + N,
     * where N is an integer initialized to 0 and automatically incremented with each class created.
     * The prefix is a string, that can be modified at runtime.
     * @return The newly created class.
     */
    private OWLClass gensymClass() {
        classCounter++;
        return datafactory.getOWLClass(IRI.create(gensymClassPrefix + classCounter));
    }

    /**
     * Creates an OWL named property. the name will be of the form PropertyPrefix + N,
     * where N is an integer initialized to 0 and automatically incremented with each property created.
     * The prefix is a string, that can be modified at runtime.
     * @return The newly created property.
     */
    public OWLObjectProperty gensymProperty() {
        propertyCounter++;
        return datafactory.getOWLObjectProperty(IRI.create(gensymPropertyPrefix + propertyCounter));
    }

    /**
     * Sets the string used as prefix of newly created owl classes.
     * @param s the string to use as prefix.
     */
    public void setGensymClassPrefix(String s) {
        if (s != null) {
            gensymClassPrefix = s;
        }
    }

    /**
     * Sets the string used as prefix of newly created owl properties.
     * @param s the string to use as prefix.
     */
    public void setGensymPropertyPrefix(String s) {
        if (s != null) {
            gensymPropertyPrefix = s;
        }
    }

    /**
     * gets the string used as prefix of newly created owl classes.
     * return The string used as prefix.
     */
    public String getGensymClassPrefix() {
        return gensymClassPrefix;
    }

    /**
     * Gets the string used as prefix of newly created owl properties.
     * return The string used as prefix.
     */
    public String getGensymPropertyPrefix() {
        return gensymPropertyPrefix;
    }

    public OWLOntology Ontology() {
        return outputOntology;
    }
    
	// checks whether an equivalent class exists for a complex class expression.
    // If so, returns it or else creates a new one and puts an entry in the hash map.
    private OWLClassExpression checkAndCreateConcept(OWLClassExpression ce) {
		if (exprConceptMap.containsKey(ce)) {
			hits++;
			return exprConceptMap.get(ce);
		}
		OWLClassExpression newConcept = gensymClass();
		exprConceptMap.put(ce, newConcept);
		return newConcept;
	}
    
    public static void main(String[] args) throws Exception {
    	if(args.length != 1) {
    		System.out.println("Specify the path of an Ontology");
    		System.exit(-1);
    	}
    	OWLOntologyManager manager = OWLManager.createOWLOntologyManager();
        File owlFile = new File(args[0]);
        IRI documentIRI = IRI.create(owlFile);
        OWLOntology ontology = manager.loadOntologyFromOntologyDocument(documentIRI);
        GregorianCalendar start = new GregorianCalendar();
    	Normalizer normalizer = new Normalizer(manager, ontology);
    	normalizer.Normalize();
    	Util.printElapsedTime(start);
    }
}
