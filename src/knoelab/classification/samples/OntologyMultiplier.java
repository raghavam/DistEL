package knoelab.classification.samples;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

import knoelab.classification.init.Normalizer;

import org.semanticweb.owlapi.apibinding.OWLManager;
import org.semanticweb.owlapi.formats.FunctionalSyntaxDocumentFormat;
import org.semanticweb.owlapi.model.IRI;
import org.semanticweb.owlapi.model.OWLAxiom;
import org.semanticweb.owlapi.model.OWLClass;
import org.semanticweb.owlapi.model.OWLDataFactory;
import org.semanticweb.owlapi.model.OWLDataProperty;
import org.semanticweb.owlapi.model.OWLDeclarationAxiom;
import org.semanticweb.owlapi.model.OWLNamedIndividual;
import org.semanticweb.owlapi.model.OWLObjectProperty;
import org.semanticweb.owlapi.model.OWLOntology;
import org.semanticweb.owlapi.model.OWLOntologyChange;
import org.semanticweb.owlapi.model.OWLOntologyManager;
import org.semanticweb.owlapi.model.OWLSubClassOfAxiom;
import org.semanticweb.owlapi.util.OWLEntityRenamer;
import org.semanticweb.owlapi.util.OWLOntologyMerger;

public class OntologyMultiplier {

	public void multiplyOntology(int num, String ontPath) throws Exception {
		OWLOntologyManager manager = OWLManager.createOWLOntologyManager();
        File owlFile = new File(ontPath);
        IRI documentIRI = IRI.create(owlFile);
        OWLOntology ontology = null;
       
        String newFileName = owlFile.getName().split("\\.")[0] + "_" + num + ".owl";
        File newOWLFile = new File(newFileName);
        IRI newOWLIRI = IRI.create(newOWLFile);
        OWLOntology newOntology = manager.createOntology(newOWLIRI);
        
        // create a new OWL file
        for(int i=1; i<=num; i++) {
        	// rename entities in the original ontology and keep adding it
        	// to the new OWL file. For each entity, add _i
        	ontology = manager.loadOntologyFromOntologyDocument(documentIRI);  
        	OWLEntityRenamer renamer = new OWLEntityRenamer(manager, Collections.singleton(ontology));
        	
        	// rename classes
        	Set<OWLClass> cls = ontology.getClassesInSignature();
        	for(OWLClass cl : cls) {
        		IRI oldClassIRI = cl.getIRI();
            	IRI newClassIRI = IRI.create(oldClassIRI.toString() + "_" + i);
            	List<OWLOntologyChange> changes = renamer.changeIRI(oldClassIRI, newClassIRI);
            	manager.applyChanges(changes);
        	}
        	       	
        	Set<OWLObjectProperty> objProps = ontology.getObjectPropertiesInSignature();
        	for(OWLObjectProperty prop : objProps) {
        		IRI oldPropIRI = prop.getIRI();
            	IRI newPropIRI = IRI.create(oldPropIRI.toString() + "_" + i);
            	List<OWLOntologyChange> changes = renamer.changeIRI(oldPropIRI, newPropIRI);
            	manager.applyChanges(changes);
        	}
        	
        	
        	Set<OWLDataProperty> dataProps = ontology.getDataPropertiesInSignature();
        	for(OWLDataProperty prop : dataProps) {
        		IRI oldPropIRI = prop.getIRI();
            	IRI newPropIRI = IRI.create(oldPropIRI.toString() + "_" + i);
            	List<OWLOntologyChange> changes = renamer.changeIRI(oldPropIRI, newPropIRI);
            	manager.applyChanges(changes);
        	}
        	
        	// copy all axioms from ontology to new ontology
        	Set<OWLAxiom> axioms = ontology.getAxioms();
        	manager.addAxioms(newOntology, axioms);
        	
        	// remove ontology from manager & reload
        	manager.removeOntology(ontology);     
        	System.out.println("Done with " + i + " copy");
        }
        manager.saveOntology(newOntology, 
        		new FunctionalSyntaxDocumentFormat(), newOWLIRI);
        System.out.println("Total Logical Axioms: " + 
        		newOntology.getLogicalAxiomCount());
 	}
	
	/**
	 * Creates an ontology which is double the size of original ontology.
	 * But the axioms are crossed. For eg., the axiom A ^ B < C will become
	 * A1 ^ B2 < C1 and A2 ^ B1 < C2. 
	 *  
	 * @param ontPath
	 */
	public void crossMultiplier(String ontPath) throws Exception {
		OWLOntologyManager manager = OWLManager.createOWLOntologyManager();
        File owlFile = new File(ontPath);
        IRI documentIRI = IRI.create(owlFile);
        OWLOntology ontology = null;
       
        String newFileName = owlFile.getName().split("\\.")[0] + "_1*2.owl";
        File newOWLFile = new File(newFileName);
        IRI newOWLIRI = IRI.create(newOWLFile);
        OWLOntology newOntology = manager.createOntology(newOWLIRI);
        
        // part-1
        System.out.println("Renaming classes...");
        {
	        // rename entities in the original ontology and keep adding it
	    	// to the new OWL file. For each entity, add _i
	    	ontology = manager.loadOntologyFromOntologyDocument(documentIRI);  
	    	OWLEntityRenamer renamer = new OWLEntityRenamer(manager, Collections.singleton(ontology));
	    	
	    	// rename classes
	    	Set<OWLClass> cls = ontology.getClassesInSignature();
	    	int count = 0;
	    	for(OWLClass cl : cls) {
	    		IRI oldClassIRI = cl.getIRI();
	        	IRI newClassIRI = null;
	        	if(count%2 == 0)
	        		newClassIRI = IRI.create(oldClassIRI.toString() + "_" + 1);
	        	else
	        		newClassIRI = IRI.create(oldClassIRI.toString() + "_" + 2);
	        	List<OWLOntologyChange> changes = renamer.changeIRI(oldClassIRI, newClassIRI);
	        	manager.applyChanges(changes);
	        	count++;
	    	}
	    	
	    	Set<OWLObjectProperty> objProps = ontology.getObjectPropertiesInSignature();
	    	for(OWLObjectProperty prop : objProps) {
	    		IRI oldPropIRI = prop.getIRI();
	        	IRI newPropIRI = IRI.create(oldPropIRI.toString() + "_" + 1);
	        	List<OWLOntologyChange> changes = renamer.changeIRI(oldPropIRI, newPropIRI);
	        	manager.applyChanges(changes);
	    	}
	    	
	    	
	    	Set<OWLDataProperty> dataProps = ontology.getDataPropertiesInSignature();
	    	for(OWLDataProperty prop : dataProps) {
	    		IRI oldPropIRI = prop.getIRI();
	        	IRI newPropIRI = IRI.create(oldPropIRI.toString() + "_" + 1);
	        	List<OWLOntologyChange> changes = renamer.changeIRI(oldPropIRI, newPropIRI);
	        	manager.applyChanges(changes);
	    	}
	    	
	    	// copy all axioms from ontology to new ontology
	    	Set<OWLAxiom> axioms = ontology.getAxioms();
	    	manager.addAxioms(newOntology, axioms);
	    	
	    	// remove ontology from manager & reload
	    	manager.removeOntology(ontology);   
        }
        
    	// repeat the same as above for the 2nd part
        {
        	// rename entities in the original ontology and keep adding it
	    	// to the new OWL file. For each entity, add _i
	    	ontology = manager.loadOntologyFromOntologyDocument(documentIRI);  
	    	OWLEntityRenamer renamer = new OWLEntityRenamer(manager, Collections.singleton(ontology));
	    	
	    	// rename classes
	    	Set<OWLClass> cls = ontology.getClassesInSignature();
	    	int count = 0;
	    	for(OWLClass cl : cls) {
	    		IRI oldClassIRI = cl.getIRI();
	        	IRI newClassIRI = null;
	        	if(count%2 == 0)
	        		newClassIRI = IRI.create(oldClassIRI.toString() + "_" + 2);
	        	else
	        		newClassIRI = IRI.create(oldClassIRI.toString() + "_" + 1);
	        	List<OWLOntologyChange> changes = renamer.changeIRI(oldClassIRI, newClassIRI);
	        	manager.applyChanges(changes);
	        	count++;
	    	}
	    	
	    	Set<OWLObjectProperty> objProps = ontology.getObjectPropertiesInSignature();
	    	for(OWLObjectProperty prop : objProps) {
	    		IRI oldPropIRI = prop.getIRI();
	        	IRI newPropIRI = IRI.create(oldPropIRI.toString() + "_" + 2);
	        	List<OWLOntologyChange> changes = renamer.changeIRI(oldPropIRI, newPropIRI);
	        	manager.applyChanges(changes);
	    	}
	    	
	    	
	    	Set<OWLDataProperty> dataProps = ontology.getDataPropertiesInSignature();
	    	for(OWLDataProperty prop : dataProps) {
	    		IRI oldPropIRI = prop.getIRI();
	        	IRI newPropIRI = IRI.create(oldPropIRI.toString() + "_" + 2);
	        	List<OWLOntologyChange> changes = renamer.changeIRI(oldPropIRI, newPropIRI);
	        	manager.applyChanges(changes);
	    	}
	    	
	    	// copy all axioms from ontology to new ontology
	    	Set<OWLAxiom> axioms = ontology.getAxioms();
	    	manager.addAxioms(newOntology, axioms);
	    	
	    	// remove ontology from manager & reload
	    	manager.removeOntology(ontology);  
        }
        manager.saveOntology(newOntology, 
        		new FunctionalSyntaxDocumentFormat(), newOWLIRI);
	}
	
	public void mergeOntologies(String[] args) throws Exception {
		File inputDir = new File(args[0]);
    	File[] allFiles = inputDir.listFiles();
		OWLOntologyManager manager = OWLManager.createOWLOntologyManager();
		int totalAxioms = 0;
/*		
		for(File owlFile : allFiles) {
	        IRI documentIRI = IRI.create(owlFile);
	        OWLOntology ontToBeMerged = 
	        	manager.loadOntologyFromOntologyDocument(documentIRI);  
	        totalAxioms +=	ontToBeMerged.getLogicalAxiomCount();
		}
*/		
		for(String ontPath : args) {
	        File owlFile = new File(ontPath);
	        IRI documentIRI = IRI.create(owlFile);
	        OWLOntology ontToBeMerged = 
	        	manager.loadOntologyFromOntologyDocument(documentIRI); 
	        totalAxioms +=	ontToBeMerged.getLogicalAxiomCount();
		}
		System.out.println("Total logical axioms:  " + totalAxioms);
		OWLOntologyMerger merger = new OWLOntologyMerger(manager);
		IRI mergedOntologyIRI = IRI.create(new File("mergedOntology123.owl"));
        OWLOntology mergedOntology = 
        	merger.createMergedOntology(manager, mergedOntologyIRI);
        System.out.println("Logical axioms in merged ontology: " + 
        		mergedOntology.getLogicalAxiomCount());
        System.out.println("Saving mergedOntology.owl....");
        manager.saveOntology(mergedOntology, new FunctionalSyntaxDocumentFormat(), 
        		mergedOntologyIRI);
	}
	
	private void addNamedIndividuals(String ontPath) throws Exception {
		OWLOntologyManager manager = OWLManager.createOWLOntologyManager();
		OWLDataFactory dataFactory = manager.getOWLDataFactory();
        File owlFile = new File(ontPath);
        IRI documentIRI = IRI.create(owlFile);
        OWLOntology ontology = manager.loadOntologyFromOntologyDocument(documentIRI);
       
        String newFileName = owlFile.getName().split("\\.")[0] + "-ind" + ".owl";
        File newOWLFile = new File(newFileName);
        IRI newOWLIRI = IRI.create(newOWLFile);
        OWLOntology newOntology = manager.createOntology(newOWLIRI);
        manager.addAxioms(newOntology, ontology.getAxioms());
        
        final int NUM_INDIVIDUALS = 1000;
        String indPrefix = "http://knoelab.wright.edu/ontologies#ClassInd";
        
        Set<OWLAxiom> indDecAxioms = new HashSet<OWLAxiom>(NUM_INDIVIDUALS);
        System.out.println("Creating named individuals...");
        for(int i=1; i<=NUM_INDIVIDUALS; i++) {
        	OWLNamedIndividual ind = 
        		dataFactory.getOWLNamedIndividual(IRI.create(indPrefix + i));
        	OWLDeclarationAxiom decAx = dataFactory.getOWLDeclarationAxiom(ind);
        	indDecAxioms.add(decAx);
        }
        manager.addAxioms(newOntology, indDecAxioms);
//        manager.saveOntology(newOntology, new RDFXMLOntologyFormat());
        System.out.println("Added " + NUM_INDIVIDUALS + " individuals.");
	}
	
	public void addABoxAssertions(String ontPath, int numAssertions) 
		throws Exception {
		/* generate 1000 new classes. Divide total ABox assertions into 70% 
         * class assertions and 30% prop assertions. Randomly pick classes 
         * from the class set and generate subClassOfAxioms (pick props randomly).
         */
		final int NUM_INDIVIDUALS = 1000;
		String indPrefix = "http://knoelab.wright.edu/ontologies#ClassInd";
		int numClassAssertions = (int)Math.ceil(0.7 * numAssertions);
		int numObjPropAssertions = numAssertions - numClassAssertions;
		OWLOntologyManager manager = OWLManager.createOWLOntologyManager();
		OWLDataFactory dataFactory = manager.getOWLDataFactory();
        File owlFile = new File(ontPath);
        IRI documentIRI = IRI.create(owlFile);
        OWLOntology ontology = manager.loadOntologyFromOntologyDocument(documentIRI);
        List<OWLClass> genClasses = new ArrayList<OWLClass>();
        for(int i=1; i<=NUM_INDIVIDUALS; i++) 
        	genClasses.add(dataFactory.getOWLClass(IRI.create(indPrefix + i)));
        Set<OWLClass> classes = ontology.getClassesInSignature();
        List<OWLClass> classList = new ArrayList<OWLClass>(classes);
        classes.clear();
        Set<OWLSubClassOfAxiom> classAssertions = 
        	new HashSet<OWLSubClassOfAxiom>(numClassAssertions);
        Random random = new Random();
        System.out.println("Generating " + numClassAssertions + " class assertions.");
        for(int i=1; i<=numClassAssertions; i++) {
        	OWLClass cindLeft = genClasses.get(random.nextInt(NUM_INDIVIDUALS));
        	OWLClass cindRight = classList.get(random.nextInt(classList.size()));
        	OWLSubClassOfAxiom subAx = dataFactory.getOWLSubClassOfAxiom(
        									cindLeft, cindRight);
        	classAssertions.add(subAx);
        }
        manager.addAxioms(ontology, classAssertions);
        classAssertions.clear();
        classList.clear();
        List<OWLObjectProperty> objProps = 
        	new ArrayList<OWLObjectProperty>(
        			ontology.getObjectPropertiesInSignature());
        Set<OWLSubClassOfAxiom> objPropAssertions = 
        	new HashSet<OWLSubClassOfAxiom>(numObjPropAssertions);
        System.out.println("Generating " + numObjPropAssertions + 
        		" obj prop assertions.");
        for(int i=1; i<=numObjPropAssertions; i++) {
        	OWLClass cindLeft = genClasses.get(random.nextInt(NUM_INDIVIDUALS));
        	OWLClass cindRight = genClasses.get(random.nextInt(NUM_INDIVIDUALS));
        	OWLObjectProperty property = objProps.get(random.nextInt(objProps.size()));
        	OWLSubClassOfAxiom subAx = dataFactory.getOWLSubClassOfAxiom(
        			cindLeft, dataFactory.getOWLObjectSomeValuesFrom(
        					property, cindRight));
        	objPropAssertions.add(subAx);
        }
        manager.addAxioms(ontology, objPropAssertions);
        objPropAssertions.clear();
        objProps.clear();
        System.out.println("Saving ontology...");
        manager.saveOntology(ontology, new FunctionalSyntaxDocumentFormat());
        System.out.println("Done");
	}
	
	public static void main(String[] args) throws Exception {
		if(args.length != 2) {
			System.out.println("Needs 2 inputs -- (multiplier, path_To_Ontology)");
//			System.out.println("Needs 2 inputs -- path_To_Ontologies and ABox assertions");
			System.exit(-1);
		}
		new OntologyMultiplier().multiplyOntology(Integer.parseInt(args[0]), args[1]);
//		new OntologyMultiplier().addABoxAssertions(args[0], Integer.parseInt(args[1]));
//		System.out.println("Output written to Original_Ontology_Name_<multiplier>.owl");
//		new OntologyMultiplier().mergeOntologies(args);
	}

}
