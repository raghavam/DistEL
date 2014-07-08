/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package knoelab.classification.init;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;
import java.util.UUID;

import knoelab.classification.misc.Constants;
import knoelab.classification.misc.HostInfo;
import knoelab.classification.misc.LRUCache;
import knoelab.classification.misc.PropertyFileHandler;
import knoelab.classification.misc.Util;

import org.semanticweb.owlapi.apibinding.OWLManager;
import org.semanticweb.owlapi.io.OWLFunctionalSyntaxOntologyFormat;
import org.semanticweb.owlapi.io.OWLXMLOntologyFormat;
import org.semanticweb.owlapi.model.AxiomType;
import org.semanticweb.owlapi.model.IRI;
import org.semanticweb.owlapi.model.OWLAxiom;
import org.semanticweb.owlapi.model.OWLClass;
import org.semanticweb.owlapi.model.OWLClassAssertionAxiom;
import org.semanticweb.owlapi.model.OWLClassExpression;
import org.semanticweb.owlapi.model.OWLDataFactory;
import org.semanticweb.owlapi.model.OWLDataPropertyAssertionAxiom;
import org.semanticweb.owlapi.model.OWLDataPropertyDomainAxiom;
import org.semanticweb.owlapi.model.OWLDataPropertyRangeAxiom;
import org.semanticweb.owlapi.model.OWLDisjointClassesAxiom;
import org.semanticweb.owlapi.model.OWLEquivalentClassesAxiom;
import org.semanticweb.owlapi.model.OWLEquivalentObjectPropertiesAxiom;
import org.semanticweb.owlapi.model.OWLIndividual;
import org.semanticweb.owlapi.model.OWLLogicalAxiom;
import org.semanticweb.owlapi.model.OWLObjectHasValue;
import org.semanticweb.owlapi.model.OWLObjectIntersectionOf;
import org.semanticweb.owlapi.model.OWLObjectOneOf;
import org.semanticweb.owlapi.model.OWLObjectProperty;
import org.semanticweb.owlapi.model.OWLObjectPropertyAssertionAxiom;
import org.semanticweb.owlapi.model.OWLObjectPropertyDomainAxiom;
import org.semanticweb.owlapi.model.OWLObjectPropertyExpression;
import org.semanticweb.owlapi.model.OWLObjectPropertyRangeAxiom;
import org.semanticweb.owlapi.model.OWLObjectSomeValuesFrom;
import org.semanticweb.owlapi.model.OWLObjectUnionOf;
import org.semanticweb.owlapi.model.OWLOntology;
import org.semanticweb.owlapi.model.OWLOntologyCreationException;
import org.semanticweb.owlapi.model.OWLOntologyManager;
import org.semanticweb.owlapi.model.OWLSubClassOfAxiom;
import org.semanticweb.owlapi.model.OWLSubObjectPropertyOfAxiom;
import org.semanticweb.owlapi.model.OWLSubPropertyChainOfAxiom;
import org.semanticweb.owlapi.model.OWLTransitiveObjectPropertyAxiom;

import redis.clients.jedis.Jedis;

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
    private String classPrefix;
    private String propertyPrefix;
    private OWLOntology inputOntology;
    private OWLOntology outputOntology;
    private PropertyFileHandler propertyFileHandler;
    
    // This hashtable is used to keep track of the complex concepts that
	// have been replaced with its equivalent simple new concepts
	private LRUCache<OWLClassExpression, OWLClassExpression> exprConceptMap;
	private int hits = 0;
	private int cacheHits = 0;
	private Jedis normalizerCache;
	private Set<String> removedTypes;
	private Map<OWLObjectPropertyExpression, Set<OWLClassExpression>> objPropRangeMap;
	private Map<String, OWLClassExpression> rangeReplacementMap;
	private Set<OWLAxiom> existentialRangeAxiomsChecked;

    /**
     * Creates a Normalizer, using the specified ontology manager and ontology.
     * @param m The manager used. It is needed to manipulate OWL axioms, classes, and properties.
     */
    public Normalizer(OWLOntologyManager m, OWLOntology inOntology) 
    throws OWLOntologyCreationException {
        manager = m;
        datafactory = m.getOWLDataFactory();
        inputOntology = inOntology;
        outputOntology = m.createOntology();
        exprConceptMap = 
        	new LRUCache<OWLClassExpression, OWLClassExpression>(10000);
        propertyFileHandler = PropertyFileHandler.getInstance();
        HostInfo cacheInfo = propertyFileHandler.getNormalizerCache();
        normalizerCache = new Jedis(cacheInfo.getHost(), cacheInfo.getPort(), 
        		Constants.INFINITE_TIMEOUT);
        classPrefix = propertyFileHandler.getClassPrefix();
        propertyPrefix = propertyFileHandler.getPropertyPrefix();
        removedTypes = new HashSet<String>();
        objPropRangeMap = 
        		new HashMap<OWLObjectPropertyExpression, Set<OWLClassExpression>>();
        rangeReplacementMap = new HashMap<String, OWLClassExpression>();
        existentialRangeAxiomsChecked = new HashSet<OWLAxiom>();
    }

    /**
     * Normalizes an EL axiom.  See stack based version for further details.
     * @param axiom The axiom to process.
     * @return The processed axiom.
     * @throws Exception 
     */
    public OWLOntology Normalize() throws Exception {
    	try { 
    		// eliminate range restrictions by transforming them as follows
    		// C < 3r.D is replaced with C < 3r.X, X < D and X < A, for all
    		// A in range(r). From "Pushing the EL Envelope Further".
    		Set<OWLObjectPropertyRangeAxiom> objPropRangeAxioms = 
    				inputOntology.getAxioms(AxiomType.OBJECT_PROPERTY_RANGE);
    		for(OWLObjectPropertyRangeAxiom ax : objPropRangeAxioms) {
    			if(!(ax.getRange() instanceof OWLClass)) 
    				continue;
    			Set<OWLClassExpression> rangeSet = 
    					objPropRangeMap.get(ax.getProperty());
    			if(rangeSet == null) {
    				rangeSet = new HashSet<OWLClassExpression>();
    				rangeSet.add(ax.getRange());
    				objPropRangeMap.put(ax.getProperty(), rangeSet);
    			}
    			else
    				rangeSet.add(ax.getRange());
    		}
    		manager.removeAxioms(inputOntology, objPropRangeAxioms);
    		
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
	        System.out.println("No of Redis cache hits: " + cacheHits + "\n");
	        exprConceptMap.clear();
	        return outputOntology;
    	}
    	finally {
    		normalizerCache.disconnect();
    	}
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
            NormalizeStatus status = NormalizePhase1(ax);
            Set<? extends OWLAxiom> tempResults = 
            	status.getPartiallyNormalizedAxioms();
            if(status.skipAxiom())
            	continue;
            else if (tempResults == null) {
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
    private NormalizeStatus NormalizePhase1(OWLAxiom ax) throws Exception {
        if (ax instanceof OWLSubPropertyChainOfAxiom) {
        	NormalizeStatus status = new NormalizeStatus(
        			NormalizePhase1((OWLSubPropertyChainOfAxiom) ax), false);
            return status;
        } 
        else if (ax instanceof OWLSubClassOfAxiom) {
        	OWLSubClassOfAxiom subClassAxiom = (OWLSubClassOfAxiom) ax;
        	OWLClassExpression subCE = subClassAxiom.getSubClass();
        	OWLClassExpression superCE = subClassAxiom.getSuperClass();
        	boolean skipAxiom = false;
        	NormalizeStatus status = null;
        	if(isAcceptableType(subCE)) {
        		if(isAcceptableType(superCE)) {
        			status = new NormalizeStatus(
                			NormalizePhase1(subClassAxiom), skipAxiom);
            	}
        		else {
        			skipAxiom = true;
        			removedTypes.add(superCE.getClass().getSimpleName());
        		}
        	}
        	else {
        		skipAxiom = true;
        		removedTypes.add(subCE.getClass().getSimpleName());
        	}
        	if(skipAxiom)
        		status = new NormalizeStatus(null, skipAxiom);
            return status;
        } 
        else if (ax instanceof OWLClassAssertionAxiom) {
        	Set<OWLAxiom> axioms = NormalizePhase1((
        			(OWLClassAssertionAxiom)ax).asOWLSubClassOfAxiom());
        	if(axioms == null) {
        		NormalizeStatus status = new NormalizeStatus(Collections.singleton((
        				(OWLClassAssertionAxiom)ax).asOWLSubClassOfAxiom()), false);
        		return status;
        	}
        	else {
        		NormalizeStatus status = new NormalizeStatus(axioms, false);
        		return status;
        	}
        }
        else if (ax instanceof OWLObjectPropertyAssertionAxiom) {
        	OWLSubClassOfAxiom subClassAxiom = 
        		((OWLObjectPropertyAssertionAxiom)ax).asOWLSubClassOfAxiom();
        	return new NormalizeStatus(Collections.singleton(subClassAxiom), false);
        }
        else if (ax instanceof OWLEquivalentClassesAxiom) {
        	Set<OWLSubClassOfAxiom> axioms = 
        		((OWLEquivalentClassesAxiom) ax).asOWLSubClassOfAxioms();
        	Set<OWLAxiom> phase1NormalizedAxioms = new HashSet<OWLAxiom>();
        	for(OWLSubClassOfAxiom axiom : axioms) {
        		Set<OWLAxiom> partiallyNormalizedAxioms = NormalizePhase1(axiom);
        		if(partiallyNormalizedAxioms == null)
        			phase1NormalizedAxioms.add(axiom);
        		else
        			phase1NormalizedAxioms.addAll(partiallyNormalizedAxioms);
        	}
        	return new NormalizeStatus(phase1NormalizedAxioms, false);
        }
        else if(ax instanceof OWLEquivalentObjectPropertiesAxiom) {
        	Set<OWLSubObjectPropertyOfAxiom> axioms = 
        			((OWLEquivalentObjectPropertiesAxiom) ax).
        			asSubObjectPropertyOfAxioms();
        	return new NormalizeStatus(axioms, false);
        }
        else if (ax instanceof OWLTransitiveObjectPropertyAxiom) {
			// convert to property chain axiom
			OWLTransitiveObjectPropertyAxiom transitivePropertyAxiom = 
								(OWLTransitiveObjectPropertyAxiom) ax;
			List<OWLObjectPropertyExpression> subPropertyList = 
								new ArrayList<OWLObjectPropertyExpression>();
			OWLObjectPropertyExpression ope = transitivePropertyAxiom.getProperty();
			subPropertyList.add(ope);
			subPropertyList.add(ope);
			OWLSubPropertyChainOfAxiom propertyChainAxiom = 
				datafactory.getOWLSubPropertyChainOfAxiom(subPropertyList,
			                ope);

        	Set<OWLAxiom> propertyChainAxiomSet = new HashSet<OWLAxiom>();
        	propertyChainAxiomSet.add(propertyChainAxiom);
			return new NormalizeStatus(propertyChainAxiomSet, false);
		}
        else if(ax instanceof OWLObjectPropertyDomainAxiom) {
        	//handle this in AxiomLoader
        	return new NormalizeStatus(null, false);
        }
//      else if(ax instanceof OWLDataPropertyDomainAxiom) {
        	//handle this in AxiomLoader
//        	return new NormalizeStatus(null, false);
//      }
        else if(ax instanceof OWLDisjointClassesAxiom) {
        	//convert this into C ^ D...^ E < \bot
        	OWLDisjointClassesAxiom disjointAxiom = (OWLDisjointClassesAxiom)ax;
        	Set<OWLClassExpression> disjointCEs = 
        		disjointAxiom.getClassExpressions();
        	OWLObjectIntersectionOf intersectionAxiom = 
        		datafactory.getOWLObjectIntersectionOf(disjointCEs);
        	OWLSubClassOfAxiom subClassAxiom = 
        		datafactory.getOWLSubClassOfAxiom(
        				intersectionAxiom, datafactory.getOWLNothing());
        	Set<OWLAxiom> partiallyNormalizedAxioms = 
        		NormalizePhase1(subClassAxiom);
        	if(partiallyNormalizedAxioms == null)
        		return new NormalizeStatus(
        				Collections.singleton(subClassAxiom), false);
        	else
        		return new NormalizeStatus(partiallyNormalizedAxioms, false);
        }
        else if(ax instanceof OWLSubObjectPropertyOfAxiom)
        	return new NormalizeStatus(null, false);
        else {
        	removedTypes.add(ax.getClass().getSimpleName());
        	return new NormalizeStatus(null, true);
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
        }
        OWLSubClassOfAxiom subClassAxiom = (OWLSubClassOfAxiom) ax;
        OWLClassExpression oce = subClassAxiom.getSuperClass();
        //check for ObjectPropertyRange elimination
        if(oce instanceof OWLObjectSomeValuesFrom) {
        	boolean isNormalForm6 = NF6Check(subClassAxiom);
        	if(!isNormalForm6) 
        		return eliminateObjPropertyRange(subClassAxiom);
        	else 
        		return NF6(subClassAxiom);
        }
        else if (NF5Check((OWLSubClassOfAxiom) ax)) {
            return NF5((OWLSubClassOfAxiom) ax);
        } else if (NF7Check((OWLSubClassOfAxiom) ax)) {
            return NF7((OWLSubClassOfAxiom) ax);
        }
        else {
            return null;
        }
    }
    
    private Set<OWLAxiom> eliminateObjPropertyRange(OWLSubClassOfAxiom ax) {
    	if(existentialRangeAxiomsChecked.contains(ax))
    		return null;
    	OWLClassExpression oce = ax.getSuperClass();
    	OWLObjectSomeValuesFrom osv = (OWLObjectSomeValuesFrom) oce;
		OWLObjectPropertyExpression ope = osv.getProperty();
		Set<OWLAxiom> results = null;
		if(objPropRangeMap.containsKey(ope)) {
			results = new HashSet<OWLAxiom>();
			//check if range has already been replaced. For eg., consider
			//A < 3r.B, D < 3r.B and let range(r) = M. Then in both cases,
			//X < B, X < M is produced. It is sufficient to do this only once.
			StringBuilder sb = new StringBuilder().append(ope.toString()).
					append(osv.getFiller());
			if(rangeReplacementMap.containsKey(sb.toString())) {
				OWLClassExpression replacementOCE = 
						rangeReplacementMap.get(sb.toString());
				OWLSubClassOfAxiom replacementAxiom = 
						datafactory.getOWLSubClassOfAxiom(ax.getSubClass(), 
						datafactory.getOWLObjectSomeValuesFrom(
								ope, replacementOCE));
				results.add(replacementAxiom);
				existentialRangeAxiomsChecked.add(replacementAxiom);
			}
			else {
				OWLClassExpression replacementOCE = gensymClass();
				rangeReplacementMap.put(sb.toString(), replacementOCE);
				OWLSubClassOfAxiom replacementAxiom = 
						datafactory.getOWLSubClassOfAxiom(ax.getSubClass(), 
						datafactory.getOWLObjectSomeValuesFrom(
								ope, replacementOCE));
				results.add(replacementAxiom);
				existentialRangeAxiomsChecked.add(replacementAxiom);
				results.add(datafactory.getOWLSubClassOfAxiom(
						replacementOCE, osv.getFiller()));
				Set<OWLClassExpression> rangeSet = objPropRangeMap.get(ope);
				for(OWLClassExpression range : rangeSet) 
					results.add(datafactory.getOWLSubClassOfAxiom(
							replacementOCE, range));
			}
		}
		return results;
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
        if (sub instanceof OWLObjectIntersectionOf) {
        	OWLObjectIntersectionOf intersectionAxiom = 
        		(OWLObjectIntersectionOf)sub;
        	Set<OWLClassExpression> operands = intersectionAxiom.getOperands();
        	for(OWLClassExpression op : operands) {
        		if(isBasic(op))
        			continue;
        		else
        			return true;
        	}
            return false;
        }
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
        if (isBasic(sub) && sup instanceof OWLObjectSomeValuesFrom) {
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
        if (isBasic(sub) && sup instanceof OWLObjectIntersectionOf) {
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
        OWLSubClassOfAxiom newAxiom = datafactory.getOWLSubClassOfAxiom(b, 
        		datafactory.getOWLObjectSomeValuesFrom(sup.getProperty(), a));
        //check whether the newly generated axiom has range restrictions
        Set<OWLAxiom> replacedAxioms = eliminateObjPropertyRange(newAxiom);
        if(replacedAxioms == null)
        	results.add(newAxiom);
        else
        	results.addAll(replacedAxioms);
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
     * Tests whether the class expression is Thing, Nothing, 
     * or a class name (i.e., not anonymous).
     * @param oce The class expression to process.
     * @return True, if it is Thing, Nothing, or a class name, and false otherwise.
     */
    private boolean isBasic(OWLClassExpression oce) {
        if (oce.isOWLNothing() || oce.isOWLThing() || (oce instanceof OWLClass)
        		|| (oce instanceof OWLIndividual)) {
            return true;
        } else {
            return false;
        }
    }

    /**
     * Creates an OWL named class. the name will be of the form ClassPrefix + N,
     * where N is a UUID.
     * The prefix is a string, that can be modified at runtime.
     * @return The newly created class.
     */
    private OWLClass gensymClass() {
        return datafactory.getOWLClass(IRI.create(classPrefix + 
        		UUID.randomUUID().toString()));
    }

    /**
     * Creates an OWL named property. the name will be of the form PropertyPrefix + N,
     * where N is a UUID.
     * The prefix is a string, that can be modified at runtime.
     * @return The newly created property.
     */
    public OWLObjectProperty gensymProperty() {
        return datafactory.getOWLObjectProperty(IRI.create(propertyPrefix + 
        		UUID.randomUUID().toString()));
    }

    /**
     * Sets the string used as prefix of newly created owl classes.
     * @param s the string to use as prefix.
     */
    public void setGensymClassPrefix(String s) {
        if (s != null) {
            classPrefix = s;
        }
    }

    /**
     * Sets the string used as prefix of newly created owl properties.
     * @param s the string to use as prefix.
     */
    public void setGensymPropertyPrefix(String s) {
        if (s != null) {
            propertyPrefix = s;
        }
    }

    /**
     * gets the string used as prefix of newly created owl classes.
     * return The string used as prefix.
     */
    public String getGensymClassPrefix() {
        return classPrefix;
    }

    /**
     * Gets the string used as prefix of newly created owl properties.
     * return The string used as prefix.
     */
    public String getGensymPropertyPrefix() {
        return propertyPrefix;
    }

    public OWLOntology Ontology() {
        return outputOntology;
    }
    
    public Set<String> getRemovedTypes() {
    	return removedTypes;
    }
    
	// checks whether an equivalent class exists for a complex class expression.
    // If so, returns it or else creates a new one and puts an entry in the hash map.
    private OWLClassExpression checkAndCreateConcept(OWLClassExpression ce) {
		if (exprConceptMap.containsKey(ce)) {
			hits++;
			return exprConceptMap.get(ce);
		}
		else {
			String classIRI = normalizerCache.get(
					ce.toString().substring(1, ce.toString().length()-1));
			if(classIRI == null || classIRI.isEmpty()) {
				OWLClassExpression newConcept = gensymClass();
				exprConceptMap.put(ce, newConcept);
				normalizerCache.set(
						ce.toString().substring(1, ce.toString().length()-1), 
						newConcept.toString().substring(1, 
								newConcept.toString().length()-1));
				return newConcept;
			}
			else {
				OWLClassExpression newConcept = 
					datafactory.getOWLClass(IRI.create(classIRI));
				exprConceptMap.put(ce, newConcept);
				cacheHits++;
				return newConcept;
			}
		}
	}
    
    public static void main(String[] args) throws Exception {
    	
    	if(args.length != 2) {
    		System.out.println("Specify input folder and output folder");
    		System.exit(-1);
    	}
    	GregorianCalendar start = new GregorianCalendar();    	
    	Set<String> typesRemoved = new HashSet<String>();
    	int totalAxioms = 0;
    	OWLFunctionalSyntaxOntologyFormat functionalFormat = 
    			new OWLFunctionalSyntaxOntologyFormat();
    	OWLOntologyManager manager = OWLManager.createOWLOntologyManager();
//    	totalAxioms += normalizeData(manager, new File(args[0]), 
//    			functionalFormat, typesRemoved, args[1]);
    	
    	File inputDir = new File(args[0]);
    	File[] allFiles = inputDir.listFiles();
    	int count = 0;
    	for(File owlFile : allFiles) {
    		totalAxioms += normalizeData(manager, owlFile, 
    				functionalFormat, typesRemoved, args[1]);
    		count++;
    		System.out.println("Done with " + count);
    	}
    	
    	System.out.println("Total axiom count: " + totalAxioms);
    	System.out.println("Removed axiom types: ");
    	for(String s : typesRemoved)
    		System.out.println(s);
    	
/*    	
    	GregorianCalendar start = new GregorianCalendar();
    	File owlFile = new File(args[0]);
    	IRI documentIRI = IRI.create(owlFile);
    	OWLOntologyManager manager = OWLManager.createOWLOntologyManager();
        OWLOntology ontology = manager.loadOntology(documentIRI);
    	Normalizer normalizer = new Normalizer(manager, ontology);
    	OWLOntology normalizedOntology = normalizer.Normalize();
    	OWLXMLOntologyFormat owlxmlFormat = new OWLXMLOntologyFormat();
	    File file = new File("norm-" + owlFile.getName());
	    if(file.exists())
	    	file.delete();
	    manager.saveOntology(normalizedOntology, 
	    		owlxmlFormat, IRI.create(file)); 
	    manager.removeOntology(ontology);
*/	    
    	System.out.println("Time taken (millis): " + 
				Util.getElapsedTime(start));
    }
    
    private static int normalizeData(OWLOntologyManager manager, 
    		File owlFile, OWLFunctionalSyntaxOntologyFormat functionalFormat, 
    		Set<String> typesRemoved, String outputPath) throws Exception {
    	IRI documentIRI = IRI.create(owlFile);
        OWLOntology ontology = manager.loadOntology(documentIRI);
    	Normalizer normalizer = new Normalizer(manager, ontology);
    	OWLOntology normalizedOntology = normalizer.Normalize();
	    File file = new File(outputPath + File.separator + 
	    		"norm-" + owlFile.getName());
	    if(file.exists())
	    	file.delete();
	    manager.saveOntology(normalizedOntology, 
	    		functionalFormat, IRI.create(file)); 
	    typesRemoved.addAll(normalizer.getRemovedTypes());
	    manager.removeOntology(ontology);
	    return normalizedOntology.getLogicalAxiomCount();
    }
    
    private class NormalizeStatus {
    	private Set<? extends OWLAxiom> partiallyNormalizedAxioms;
    	private boolean skipAxiom;
    	
    	NormalizeStatus(Set<? extends OWLAxiom> axioms, 
    			boolean skipAxiom) {
			partiallyNormalizedAxioms = axioms;
			this.skipAxiom = skipAxiom;
		}
    	
    	Set<? extends OWLAxiom> getPartiallyNormalizedAxioms() {
    		return partiallyNormalizedAxioms;
    	}
    	
    	boolean skipAxiom() {
    		return skipAxiom;
    	}
    }
}
