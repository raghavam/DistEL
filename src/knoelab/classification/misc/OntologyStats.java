package knoelab.classification.misc;

import java.io.File;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.coode.owlapi.owlxmlparser.OWLSubClassAxiomElementHandler;
import org.semanticweb.owlapi.apibinding.OWLManager;
import org.semanticweb.owlapi.io.OWLFunctionalSyntaxOntologyFormat;
import org.semanticweb.owlapi.model.AxiomType;
import org.semanticweb.owlapi.model.IRI;
import org.semanticweb.owlapi.model.OWLAxiom;
import org.semanticweb.owlapi.model.OWLClass;
import org.semanticweb.owlapi.model.OWLClassAssertionAxiom;
import org.semanticweb.owlapi.model.OWLClassExpression;
import org.semanticweb.owlapi.model.OWLDataExactCardinality;
import org.semanticweb.owlapi.model.OWLDataFactory;
import org.semanticweb.owlapi.model.OWLDataHasValue;
import org.semanticweb.owlapi.model.OWLDataMaxCardinality;
import org.semanticweb.owlapi.model.OWLDataProperty;
import org.semanticweb.owlapi.model.OWLDataPropertyAssertionAxiom;
import org.semanticweb.owlapi.model.OWLDataPropertyDomainAxiom;
import org.semanticweb.owlapi.model.OWLDataSomeValuesFrom;
import org.semanticweb.owlapi.model.OWLDatatypeDefinitionAxiom;
import org.semanticweb.owlapi.model.OWLDisjointClassesAxiom;
import org.semanticweb.owlapi.model.OWLIndividual;
import org.semanticweb.owlapi.model.OWLLogicalAxiom;
import org.semanticweb.owlapi.model.OWLNamedIndividual;
import org.semanticweb.owlapi.model.OWLObjectExactCardinality;
import org.semanticweb.owlapi.model.OWLObjectHasValue;
import org.semanticweb.owlapi.model.OWLObjectIntersectionOf;
import org.semanticweb.owlapi.model.OWLObjectMaxCardinality;
import org.semanticweb.owlapi.model.OWLObjectOneOf;
import org.semanticweb.owlapi.model.OWLObjectProperty;
import org.semanticweb.owlapi.model.OWLObjectPropertyAssertionAxiom;
import org.semanticweb.owlapi.model.OWLObjectPropertyDomainAxiom;
import org.semanticweb.owlapi.model.OWLObjectPropertyExpression;
import org.semanticweb.owlapi.model.OWLObjectPropertyRangeAxiom;
import org.semanticweb.owlapi.model.OWLObjectSomeValuesFrom;
import org.semanticweb.owlapi.model.OWLObjectUnionOf;
import org.semanticweb.owlapi.model.OWLOntology;
import org.semanticweb.owlapi.model.OWLOntologyManager;
import org.semanticweb.owlapi.model.OWLSubClassOfAxiom;
import org.semanticweb.owlapi.model.OWLSubDataPropertyOfAxiom;
import org.semanticweb.owlapi.model.OWLSubObjectPropertyOfAxiom;
import org.semanticweb.owlapi.model.OWLSubPropertyChainOfAxiom;


/**
 * This class prints the ontology stats
 * @author Raghava
 *
 */
public class OntologyStats {
	
	public void printOntologyStats(String ontoFile) throws Exception {
		OWLOntologyManager manager = OWLManager.createOWLOntologyManager();

		int type1 = 0, type2 = 0, type3 = 0, type4 = 0, type6 = 0, 
				type7 = 0, type8 = 0;
//		File dir = new File(ontoFile);
//		File[] files = dir.listFiles();
		
        File owlFile = new File(ontoFile);
        IRI documentIRI = IRI.create(owlFile);
        OWLOntology ontology = manager.loadOntology(documentIRI);

        Set<OWLLogicalAxiom> axioms = ontology.getLogicalAxioms();
        System.out.println("LogicalAxioms: " + axioms.size());
        System.out.println("No. of Classes: " + 
        		ontology.getClassesInSignature().size());
        System.out.println("No of Object properties: " + 
        		ontology.getObjectPropertiesInSignature().size());
        System.out.println("No of Data properties: " + 
        		ontology.getDataPropertiesInSignature().size());
        
        for(OWLLogicalAxiom ax : axioms) {
        	if(ax instanceof OWLSubClassOfAxiom) {
        		OWLSubClassOfAxiom sax = (OWLSubClassOfAxiom) ax;
        		OWLClassExpression supOCE = sax.getSuperClass();
        		OWLClassExpression subOCE = sax.getSubClass();
        		if(subOCE instanceof OWLClass) {
        			if(supOCE instanceof OWLClass)
        				type1++;
        			else if(supOCE instanceof OWLObjectSomeValuesFrom)
        				type3++;
        		}
        		else if(subOCE instanceof OWLObjectIntersectionOf) {
        			if(supOCE instanceof OWLClass)
        				type2++;
        		}
        		else if(subOCE instanceof OWLObjectSomeValuesFrom) {
        			if(supOCE instanceof OWLClass)
        				type4++;
        		}
        	}
        	else if(ax instanceof OWLSubObjectPropertyOfAxiom)
        		type7++;
        	else if(ax instanceof OWLSubPropertyChainOfAxiom)
        		type8++;
        }
        int total = type1 + type2 + type3 + type4 + type6 + type7 + type8;
        System.out.println("Type1 axioms: " + type1 + "  Type2 axioms: " + type2);
        System.out.println("Type3 axioms: " + type3 + "  Type4 axioms: " + type4);
        System.out.println("Type6 axioms: " + type6 + "  Type7 axioms: " + type7);
        System.out.println("Type8 axioms: " + type8 + "  Total: " + total);
	}
	
	public void dublinTrafficStats(String dirPath) throws Exception {
		OWLOntologyManager manager = OWLManager.createOWLOntologyManager();
		int type1 = 0, type2 = 0, type3 = 0, type4 = 0, type6 = 0, 
				type7 = 0, type8 = 0, type11 = 0, type12 = 0, type31 = 0, 
				type13 = 0, domain = 0, type32 = 0, type33NotConsidered = 0;
		int totalAxioms = 0, totalType1 = 0, totalType2 = 0, totalType3 = 0,
				totalType4 = 0, totalType6 = 0, totalType7 = 0, totalType8 = 0,
				totalType11 = 0, totalType12 = 0, totalType31 = 0, totalType13 = 0,
				totalDomain = 0, totalType32 = 0, totalType33 = 0;
		
		File dirFile = new File(dirPath);
		File[] allFiles = dirFile.listFiles();
		int fileCount = 1;
		Set<String> remTypes = new HashSet<String>();
		Set<OWLClass> classes = new HashSet<OWLClass>();
		Set<OWLObjectProperty> objProps = new HashSet<OWLObjectProperty>();
		Set<OWLDataProperty> dataProps = new HashSet<OWLDataProperty>();
		for(File owlFile : allFiles) {
			System.out.println("\n" + fileCount + "   " + owlFile.getName());
			IRI documentIRI = IRI.create(owlFile);
	        OWLOntology ontology = manager.loadOntology(documentIRI);
	        Set<OWLLogicalAxiom> axioms = ontology.getLogicalAxioms();
	        totalAxioms += axioms.size();
	        System.out.println("LogicalAxioms: " + axioms.size());
	        System.out.println("Classes: " + 
	        		ontology.getClassesInSignature().size());
	        classes.addAll(ontology.getClassesInSignature());
	        System.out.println("Object properties: " + 
	        		ontology.getObjectPropertiesInSignature().size());
	        objProps.addAll(ontology.getObjectPropertiesInSignature());
	        System.out.println("Data properties: " + 
	        		ontology.getDataPropertiesInSignature().size());
	        dataProps.addAll(ontology.getDataPropertiesInSignature());
	        
	        for(OWLLogicalAxiom ax : axioms) {
	        	if(ax instanceof OWLSubClassOfAxiom) {
	        		OWLSubClassOfAxiom sax = (OWLSubClassOfAxiom) ax;
	        		OWLClassExpression supOCE = sax.getSuperClass();
	        		OWLClassExpression subOCE = sax.getSubClass();
	        		if(subOCE instanceof OWLClass) {
	        			if(supOCE instanceof OWLClass)
	        				type1++;
	        			else if(supOCE instanceof OWLObjectSomeValuesFrom)
	        				type3++;
	        			else if(supOCE instanceof OWLObjectHasValue)
	        				type32++;
	        			else if(supOCE instanceof OWLDataHasValue)
	        				type33NotConsidered++;
	        			else
	        				remTypes.add("1 " + supOCE.getClass().getSimpleName());
	        		}
	        		else if(subOCE instanceof OWLObjectOneOf) {
	        			if(supOCE instanceof OWLObjectOneOf)
	        				type13++;
	        			else if(supOCE instanceof OWLClass)
	        				type11++;
	        			else if(supOCE instanceof OWLObjectHasValue)
	        				type12++;
	        			else if(supOCE instanceof OWLObjectSomeValuesFrom)
	        				type31++;
	        			else
	        				remTypes.add("2 " + supOCE.getClass().getSimpleName());
	        		}
	        		else if(subOCE instanceof OWLObjectIntersectionOf) {
	        			if(supOCE instanceof OWLClass)
	        				type2++;
	        		}
	        		else if(subOCE instanceof OWLObjectSomeValuesFrom) {
	        			if(supOCE instanceof OWLClass)
	        				type4++;
	        		}
	        	}
	        	else if(ax instanceof OWLSubObjectPropertyOfAxiom)
	        		type7++;
	        	else if(ax instanceof OWLSubPropertyChainOfAxiom)
	        		type8++;
	        	else if(ax instanceof OWLObjectPropertyDomainAxiom)
	        		domain++;
	        	else
	        		remTypes.add("3 " + ax.getClass().getSimpleName());
	        }
	        totalType1 += type1; totalType2 += type2; totalType3 += type3;
	        totalType4 += type4; totalType6 += type6; totalType7 += type7;
	        totalType8 += type8; totalType11 += type11; totalType12 += type12;
	        totalType31 += type31; totalDomain += domain; totalType13 += type13;
	        totalType32 += type32; totalType33 += type33NotConsidered;
	        
	        int total = type1 + type2 + type3 + type4 + type6 + type7 + type8 + 
	        		type11 + type12 + type31 + type13 + domain + 
	        		type32 + type33NotConsidered;
	        System.out.println("Type1: " + type1 + "  Type11: " + type11 + 
	        		"  Type12: " + type12 + "  Type13: " + type13);
	        System.out.println("Type2: " + type2);
	        System.out.println("Type3: " + type3 + "  Type31: " + type31 + 
	        		"  Type32: " + type32 + 
	        		"  DataHasValue: " + type33NotConsidered);
	        System.out.println("Type4: " + type4 + "  Type6: " + type6 + 
	        		"  Type7: " + type7);
	        System.out.println("Type8: " + type8 + "  Domain: " + domain +
	        		"  Total: " + total);
	        System.out.println("-------------------------------");
	        
	        manager.removeOntology(ontology);
	        fileCount++;
	        type1 = type2 = type3 = type4 = type6 = type7 = type8 = type11 = 
	        		type12 = type31 = type13 = domain = type32 = 
	        		type33NotConsidered = 0;
//	        if(fileCount == 4)
//	        	break;
		}
		System.out.println("\nTotals...");
		System.out.println("TotalType1: " + totalType1 + "  TotalType11: " + totalType11 + 
        		"  TotalType12: " + totalType12 + "  TotalType13: " + totalType13);
        System.out.println("TotalType2: " + totalType2);
        System.out.println("TotalType3: " + totalType3 + "  TotalType31: " + totalType31 + 
        		"  TotalType32: " + totalType32 + 
        		"  TotalDataHasValue: " + totalType33);
        System.out.println("TotalType4: " + totalType4 + "  TotalType6: " + totalType6 + 
        		"  TotalType7: " + totalType7);
        System.out.println("TotalType8: " + totalType8 + "  TotalDomain: " + totalDomain);
        System.out.println("Total axioms: " + totalAxioms + 
        		"    Total axioms considered: " + (totalAxioms - totalType33));
        System.out.println("Total classes: " + classes.size());
        System.out.println("Total Obj Props: " + objProps.size() + 
        		"   Total Data Props: " + dataProps.size());
        if(!remTypes.isEmpty()) {
	        System.out.println("\nRemaining types: ");
	        for(String s : remTypes)
	        	System.out.println(s);
        }
	}

	public void searchForIndividuals(String ontoFile) throws Exception {
		OWLOntologyManager manager = OWLManager.createOWLOntologyManager();
		IRI documentIRI = IRI.create(new File(ontoFile));
        OWLOntology ontology = manager.loadOntology(documentIRI);
        Set<OWLLogicalAxiom> axioms = ontology.getLogicalAxioms();
        System.out.println("Logical Axioms: " + axioms.size());
        List<IntWrapper> type1CountList = new ArrayList<IntWrapper>();
        List<IntWrapper> type2CountList = new ArrayList<IntWrapper>();
        List<IntWrapper> type3CountList = new ArrayList<IntWrapper>();
        List<IntWrapper> type4CountList = new ArrayList<IntWrapper>();
        List<IntWrapper> type5CountList = new ArrayList<IntWrapper>();
        int domainCount = 0, dataDomainCount = 0;
        int rangeCount = 0;
        int dataSomeValsCount = 0;
        int subObjPropCount = 0, subPropChainCount = 0;
        initTypeList(type1CountList);
        initTypeList(type2CountList);
        initTypeList(type3CountList);
        initTypeList(type4CountList);
        initTypeList(type5CountList);
        for(OWLLogicalAxiom ax : axioms) {
//        	System.out.println(ax);
        	if(ax instanceof OWLSubClassOfAxiom) {
        		OWLSubClassOfAxiom sax = (OWLSubClassOfAxiom) ax;
        		OWLClassExpression subClass = sax.getSubClass();
        		OWLClassExpression supClass = sax.getSuperClass();
        		if(subClass instanceof OWLObjectOneOf)
        			checkSuperClass(supClass, type1CountList);
        		else if(subClass instanceof OWLClass) {
        			checkSuperClass(supClass, type2CountList);
        		}
        		else if(subClass instanceof OWLObjectIntersectionOf) {
        			checkSuperClass(supClass, type3CountList);
        		}
        		else if(subClass instanceof OWLObjectSomeValuesFrom) {
        			checkSuperClass(supClass, type4CountList);
        		}
        		else if(subClass instanceof OWLObjectHasValue) {
        			checkSuperClass(supClass, type5CountList);
        		}
        		else if(subClass instanceof OWLDataSomeValuesFrom)
            		dataSomeValsCount++;
        		else
        			throw new Exception("Axiom is of unknown type: " + 
            				ax.getClass().getCanonicalName() + "  Axiom is: " + ax);
        	}
        	else if(ax instanceof OWLObjectPropertyDomainAxiom)
        		domainCount++;
        	else if(ax instanceof OWLDataPropertyDomainAxiom)
        		dataDomainCount++;
        	else if(ax instanceof OWLObjectPropertyRangeAxiom)
        		rangeCount++;
        	else if(ax instanceof OWLSubObjectPropertyOfAxiom)
        		subObjPropCount++;
        	else if(ax instanceof OWLSubPropertyChainOfAxiom)
        		subPropChainCount++;
        	else
        		throw new Exception("Axiom is of type: " + 
        				ax.getClass().getCanonicalName() + "  Axiom is: " + ax);
        }
        System.out.println("\nType1: ");
        for(int i=0; i<8; i++) {
        	IntWrapper intWrapper = type1CountList.get(i);
        	System.out.print("Count" + (i+1) + ": " + 
        			(intWrapper==null?0:intWrapper.n) + "  ");
        }
        System.out.println("\nType2:");
        for(int i=0; i<8; i++) {
        	IntWrapper intWrapper = type2CountList.get(i);
        	System.out.print("Count" + (i+1) + ": " + 
        			(intWrapper==null?0:intWrapper.n) + "  ");
        }
        System.out.println("\nType3: ");
        for(int i=0; i<8; i++) {
        	IntWrapper intWrapper = type3CountList.get(i);
        	System.out.print("Count" + (i+1) + ": " + 
        			(intWrapper==null?0:intWrapper.n) + "  ");
        }
        System.out.println("\nType4: ");
        for(int i=0; i<8; i++) {
        	IntWrapper intWrapper = type4CountList.get(i);
        	System.out.print("Count" + (i+1) + ": " + 
        			(intWrapper==null?0:intWrapper.n) + "  ");
        }
        System.out.println("\nType5: ");
        for(int i=0; i<8; i++) {
        	IntWrapper intWrapper = type5CountList.get(i);
        	System.out.print("Count" + (i+1) + ": " + 
        			(intWrapper==null?0:intWrapper.n) + "  ");
        }
        System.out.println("DomainCount: " + domainCount + 
        		"  DataDomainCount: " + dataDomainCount);
        System.out.println("RangeCount: " + rangeCount);
        System.out.println("DataSomeValsCount: " + dataSomeValsCount);
        System.out.println("SubObjPropCount: " + subObjPropCount + 
        		"  SubPropChainCount: " + subPropChainCount);
	}
	
	private void initTypeList(List<IntWrapper> typeCountList) {
		for(int i=0; i<8; i++)
			typeCountList.add(new IntWrapper(0));
	}
	
	private void checkSuperClass(OWLClassExpression supClass, 
			List<IntWrapper> typeCountList) {
		if(supClass instanceof OWLClass)
			typeCountList.get(0).increment();
		else if(supClass instanceof OWLObjectOneOf)
			typeCountList.get(1).increment();
		else if(supClass instanceof OWLObjectSomeValuesFrom) {
			OWLObjectSomeValuesFrom osv = (OWLObjectSomeValuesFrom) supClass;
			OWLClassExpression oce = osv.getFiller();
			if(oce instanceof OWLClass)
				typeCountList.get(2).increment();
			else if(oce instanceof OWLObjectOneOf)
				typeCountList.get(3).increment();
		}
		else if(supClass instanceof OWLDataSomeValuesFrom)
			typeCountList.get(4).increment();
		else if(supClass instanceof OWLObjectHasValue) {
			typeCountList.get(5).increment();
		}
		else if(supClass instanceof OWLDataHasValue)
			typeCountList.get(6).increment();
		else if(supClass instanceof OWLObjectExactCardinality ||
				supClass instanceof OWLObjectMaxCardinality ||
				supClass instanceof OWLDataExactCardinality ||
				supClass instanceof OWLDataMaxCardinality ||
				supClass instanceof OWLObjectUnionOf)
			typeCountList.get(7).increment();
		else
			System.out.println("SuperClass is of unknown type: " + 
    				supClass.getClass().getCanonicalName() + 
    				"  SuperClass is: " + supClass);
	}
	
	public static void main(String[] args) throws Exception {
//		if(args.length != 1) {
//			System.out.println("Enter path to the Ontology");
//			System.exit(-1);
//		}
//		new OntologyStats().printOntologyStats(args[0]);
		new OntologyStats().dublinTrafficStats(args[0]);
//		new OntologyStats().searchForIndividuals(args[0]);
	}
	
	class IntWrapper {
		int n;
		
		IntWrapper(int number) {
			this.n = number;
		}
		
		void increment() {
			n++;
		}
	}

}
