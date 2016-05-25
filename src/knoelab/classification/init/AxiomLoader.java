package knoelab.classification.init;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import knoelab.classification.misc.AxiomDB;
import knoelab.classification.misc.Constants;
import knoelab.classification.misc.HostInfo;
import knoelab.classification.misc.LRUCache;
import knoelab.classification.misc.PropertyFileHandler;
import knoelab.classification.misc.ScoreComparator;
import knoelab.classification.misc.Util;
import knoelab.classification.pipeline.PipelineManager;

import org.semanticweb.owlapi.apibinding.OWLManager;
import org.semanticweb.owlapi.io.RDFXMLOntologyFormat;
import org.semanticweb.owlapi.model.AxiomType;
import org.semanticweb.owlapi.model.IRI;
import org.semanticweb.owlapi.model.OWLClass;
import org.semanticweb.owlapi.model.OWLClassExpression;
import org.semanticweb.owlapi.model.OWLDataHasValue;
import org.semanticweb.owlapi.model.OWLDataProperty;
import org.semanticweb.owlapi.model.OWLDataPropertyAssertionAxiom;
import org.semanticweb.owlapi.model.OWLDataPropertyDomainAxiom;
import org.semanticweb.owlapi.model.OWLDataSomeValuesFrom;
import org.semanticweb.owlapi.model.OWLIndividual;
import org.semanticweb.owlapi.model.OWLLogicalAxiom;
import org.semanticweb.owlapi.model.OWLNamedIndividual;
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
import org.semanticweb.owlapi.model.OWLOntologyManager;
import org.semanticweb.owlapi.model.OWLOntologyStorageException;
import org.semanticweb.owlapi.model.OWLSubClassOfAxiom;
import org.semanticweb.owlapi.model.OWLSubDataPropertyOfAxiom;
import org.semanticweb.owlapi.model.OWLSubObjectPropertyOfAxiom;
import org.semanticweb.owlapi.model.OWLSubPropertyChainOfAxiom;
import org.semanticweb.owlapi.util.SimpleIRIMapper;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisShardInfo;
import redis.clients.jedis.Response;
import redis.clients.jedis.ShardedJedis;
import redis.clients.util.Hashing;

/**
 * Splits the axioms based on their type and 
 * assigns the axioms to a node in the cluster. 
 * 
 * @author Raghava
 *
 */

// TODO: Can initialization code S(X) = {X} be multi-threaded (or separate process)?
public class AxiomLoader {
	
	private Map<AxiomDistributionType, List<HostInfo>> typeHostMap;
	private PipelineManager pipelineManager;
	private PipelineManager pipelineConjunctIndexManager;
	private PropertyFileHandler propertyFileHandler;
	private Jedis idReader;
	private AxiomDB axiomDB;
	private Map<AxiomDistributionType, Set<OWLLogicalAxiom>> typeAxiomsMap;
	private LRUCache<String, String> conceptIDCache;
	private int numTypes;
	private boolean foundBottom;
	private long conceptRoleCount = 1;
	private double currentIncrement = 0;
	private String inputOntFile;
	private boolean isNormalized;
	private boolean isIncrementalData;
	private boolean isTripsData;
	private Map<AxiomDistributionType, Double> typesWeightMap;
	private List<AxiomDistributionType> axiomTypes;
	
	public AxiomLoader(String ontoFile, boolean isNormalized, 
			boolean isIncrementalData, boolean isTripsData) throws Exception {
		this.inputOntFile = ontoFile;
		this.isNormalized = isNormalized;
		this.isIncrementalData = isIncrementalData;
		this.isTripsData = isTripsData;
		
		propertyFileHandler = PropertyFileHandler.getInstance();
		typeAxiomsMap = new HashMap<AxiomDistributionType, 
							Set<OWLLogicalAxiom>>();
		typeHostMap = new HashMap<AxiomDistributionType, List<HostInfo>>();	
		typesWeightMap = propertyFileHandler.getAllTypesWeightMap();
		axiomTypes = 
				new ArrayList<AxiomDistributionType>(typesWeightMap.keySet());
		axiomDB = AxiomDB.NON_ROLE_DB;
		conceptIDCache = new LRUCache<String, String>(1000000);
		//subtract ID node and result node
		numTypes = AxiomDistributionType.values().length - 2;
		HostInfo idReaderInfo = propertyFileHandler.getConceptIDNode();
		idReader = new Jedis(idReaderInfo.getHost(), idReaderInfo.getPort(), 
				Constants.INFINITE_TIMEOUT);
		String bottomStr = idReader.get(Constants.FOUND_BOTTOM);
		if((bottomStr == null) || bottomStr.equals("0"))
			foundBottom = false;
		else if((bottomStr != null) && bottomStr.equals("1"))
			foundBottom = true;
		String currentIncStr = idReader.get(Constants.CURRENT_INCREMENT);
		if(currentIncStr != null)
			currentIncrement = Double.parseDouble(currentIncStr);
		else
			idReader.incr(Constants.CURRENT_INCREMENT); //sets the key to 1
	}
	
	public void loadAxioms() throws Exception {		
		OWLOntologyManager manager = OWLManager.createOWLOntologyManager();
		if(isIncrementalData) {
			idReader.incr(Constants.CURRENT_INCREMENT);
			if(isTripsData)
				addTripsBaseOntologies(manager);
		}
        File owlFile = new File(inputOntFile);
        IRI documentIRI = IRI.create(owlFile);
        OWLOntology ontology = 
        	manager.loadOntologyFromOntologyDocument(documentIRI);
        OWLOntology normalizedOntology = ontology;
        if(!isNormalized) {
        	Normalizer normalizer = new Normalizer(manager, ontology);
        	normalizedOntology = normalizer.Normalize();
//        	saveNormalizedOntology(owlFile, manager, normalizedOntology);
        }
        System.out.println("Logical axioms: " + 
        				normalizedOntology.getLogicalAxiomCount());
        System.out.println("Concepts: " + 
        				normalizedOntology.getClassesInSignature().size());
    	System.out.println("Individuals: " + 
    			normalizedOntology.getIndividualsInSignature().size());
        if(isIncrementalData) {
        	/* need not assign nodes to rules again -- just do it once before
        	 * the increments start. New axioms should go to the same node as
        	 * old axioms
        	 */
        	if(!isTripsData) {
        		//special case where isIncrement is true and isTripsData is 
        		//false, although this is about trips data. This is done only
        		//for base traffic data.
        		assignNodesToRules(normalizedOntology);
        	}
        	else {
	        	//typeHostMap & typeAxiomsMap should be filled up.
	        	categorizeAxiomsIntoTypes(normalizedOntology);
	        	for(AxiomDistributionType type : 
	        							AxiomDistributionType.values()) {
	        		Set<String> strHosts = idReader.zrange(type.toString(), 
	        				Constants.RANGE_BEGIN, Constants.RANGE_END);
	        		List<HostInfo> hosts = new ArrayList<HostInfo>(
	        									strHosts.size());
	        		for(String s : strHosts) {
	        			String[] hostPortSplit = s.split(":");
	        			hosts.add(new HostInfo(hostPortSplit[0], 
	        					Integer.parseInt(hostPortSplit[1])));
	        		}
	        		typeHostMap.put(type, hosts);
	        	}
	        	List<HostInfo> allHosts = new ArrayList<HostInfo>();
	        	Collection<List<HostInfo>> hostInfoCollection = typeHostMap.values();
	    		for(List<HostInfo> hinfoList : hostInfoCollection)
	    			allHosts.addAll(hinfoList);
	    		pipelineManager = new PipelineManager(allHosts, 
	    				propertyFileHandler.getPipelineQueueSize());
	        	pipelineConjunctIndexManager = new PipelineManager(
						typeHostMap.get(AxiomDistributionType.CR_TYPE1_2), 
						propertyFileHandler.getPipelineQueueSize());
        	}
        }
        else
        	assignNodesToRules(normalizedOntology);
        mapConceptToID(normalizedOntology);
        // Results and Concept IDs are only on 1 machine
        pipelineManager.synchAll(axiomDB);
//        pipelineManager.selectiveSynch(axiomDB, 
//        		typeHostMap.get(AxiomDistributionType.CONCEPT_ID).get(0), 
//        		typeHostMap.get(AxiomDistributionType.RESULT_TYPE).get(0));
      
//        StopWatch stopWatch = new StopWatch();
//        stopWatch.start();
        
        loadNormalizedAxioms(normalizedOntology);	
        
//        stopWatch.stopAndPrint();
        
        // close all DB connections
        idReader.disconnect();
        pipelineManager.synchAndCloseAll(axiomDB);
        pipelineConjunctIndexManager.synchAndCloseAll(AxiomDB.CONJUNCT_INDEX_DB);        
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
	
	private void saveNormalizedOntology(File owlFile, 
			OWLOntologyManager manager, OWLOntology normalizedOntology) 
			throws OWLOntologyStorageException {
    	// saving the normalized ontology to file, for testing
    	RDFXMLOntologyFormat owlxmlFormat = new RDFXMLOntologyFormat();
	    File file = new File("Norm-" + owlFile.getName());
	    if(file.exists())
	    	file.delete();
	    manager.saveOntology(normalizedOntology, owlxmlFormat, IRI.create(file)); 
	}
	
	private void assignNodesToRules(
			OWLOntology normalizedOntology) throws Exception {
		
		List<HostInfo> availableNodes = propertyFileHandler.getAllNodes();
		if(availableNodes.size() < numTypes)
			throw new Exception("Available nodes are less than required: " + 
					availableNodes.size());
		
		categorizeAxiomsIntoTypes(normalizedOntology);
		int i = initialMinAssignment(axiomTypes, availableNodes);
		typeHostMap.put(AxiomDistributionType.CONCEPT_ID, 
				Collections.singletonList(
						propertyFileHandler.getConceptIDNode()));
		typeHostMap.put(AxiomDistributionType.RESULT_TYPE, 
				Collections.singletonList(
						propertyFileHandler.getResultNode()));
		availableNodes.subList(0, i).clear();
		if(availableNodes.isEmpty()) {
			initialize();
			return;
		}
		
		Map<AxiomDistributionType, Double> typeScoreMap = 
			new HashMap<AxiomDistributionType, Double>();
		double score;
		computeScores(typeScoreMap, axiomTypes, typesWeightMap);
		Collection<Double> scores = typeScoreMap.values();
		double totalScore = 0;
		for(Double s : scores)
			totalScore += s;
		
		// replace score with number of nodes that could be assigned
		for(AxiomDistributionType type : axiomTypes) {
			score = typeScoreMap.get(type);
			score = (score/totalScore) * availableNodes.size();
			typeScoreMap.put(type, score);
		}
		
		//sort the scores and assign nodes
		sortScoresAndAssignNodes(typeScoreMap, availableNodes);
		initialize();
	}	
	
	private int initialMinAssignment(List<AxiomDistributionType> axiomTypes, 
			List<HostInfo> availableNodes) {
		// If there are axioms under a type, assign one node.
		// Dependencies: T11 <- DependsOn(T12, T32); T12 <- DependsOn(T11, T32)
		// T2 <- DependsOn(T11, T12, T32); T31 <- DependsOn(T11, T12, T32)
		// T32 <- DependsOn(T2, T4, T5); T4 <- DependsOn(T2, T5) 
		// T5 <- Depends(T2, T4); TBot <- DependsOn(T2, T5) and whether Bottom is in class list.
		// TBot doesn't depend on T4 since the value of 'r' doesn't matter, only (X,Y) matters.
		int i = 0;
		for(AxiomDistributionType type : axiomTypes) {
			List<HostInfo> nodes = new ArrayList<HostInfo>();
			if(!typeAxiomsMap.get(type).isEmpty()) {
				nodes.add(availableNodes.get(i));
				i++;
				typeHostMap.put(type, nodes);
			}
			else {
				//check if dependencies are not empty
				if(type == AxiomDistributionType.CR_TYPE1_1) {
					if(!typeAxiomsMap.get(AxiomDistributionType.CR_TYPE1_2).isEmpty() ||
					   !typeAxiomsMap.get(AxiomDistributionType.CR_TYPE2).isEmpty() ||
					   !typeAxiomsMap.get(AxiomDistributionType.CR_TYPE4).isEmpty() ||
					   !typeAxiomsMap.get(AxiomDistributionType.CR_TYPE5).isEmpty()) {
							nodes.add(availableNodes.get(i));
							i++;
							typeHostMap.put(type, nodes);
					}
				}
				else if(type == AxiomDistributionType.CR_TYPE1_2) {
					if(!typeAxiomsMap.get(AxiomDistributionType.CR_TYPE1_1).isEmpty() ||
					   !typeAxiomsMap.get(AxiomDistributionType.CR_TYPE2).isEmpty() ||
					   !typeAxiomsMap.get(AxiomDistributionType.CR_TYPE4).isEmpty() ||
					   !typeAxiomsMap.get(AxiomDistributionType.CR_TYPE5).isEmpty()) {
					 		nodes.add(availableNodes.get(i));
					 		i++;
							typeHostMap.put(type, nodes);
					}
				}
				else if(type == AxiomDistributionType.CR_TYPE2 || 
						type == AxiomDistributionType.CR_TYPE3_1) {
					if(!typeAxiomsMap.get(AxiomDistributionType.CR_TYPE1_1).isEmpty() ||
					   !typeAxiomsMap.get(AxiomDistributionType.CR_TYPE1_2).isEmpty() ||
					   !typeAxiomsMap.get(AxiomDistributionType.CR_TYPE2).isEmpty() ||
					   !typeAxiomsMap.get(AxiomDistributionType.CR_TYPE4).isEmpty() ||
					   !typeAxiomsMap.get(AxiomDistributionType.CR_TYPE5).isEmpty()) {
							nodes.add(availableNodes.get(i));
							i++;
							typeHostMap.put(type, nodes);
					}
				}
				else if(type == AxiomDistributionType.CR_TYPE3_2) {
					if(!typeAxiomsMap.get(AxiomDistributionType.CR_TYPE2).isEmpty() ||
					   !typeAxiomsMap.get(AxiomDistributionType.CR_TYPE4).isEmpty() ||
					   !typeAxiomsMap.get(AxiomDistributionType.CR_TYPE5).isEmpty()) {
							nodes.add(availableNodes.get(i));
							i++;
							typeHostMap.put(type, nodes);
					}
				}
				else if(type == AxiomDistributionType.CR_TYPE4) {
					if(!typeAxiomsMap.get(AxiomDistributionType.CR_TYPE2).isEmpty() ||
					   !typeAxiomsMap.get(AxiomDistributionType.CR_TYPE5).isEmpty()) {
							nodes.add(availableNodes.get(i));
							i++;
							typeHostMap.put(type, nodes);
					}
				}
				else if(type == AxiomDistributionType.CR_TYPE5) {
					if(!typeAxiomsMap.get(AxiomDistributionType.CR_TYPE2).isEmpty() ||
					   !typeAxiomsMap.get(AxiomDistributionType.CR_TYPE4).isEmpty()) {
							nodes.add(availableNodes.get(i));
							i++;
							typeHostMap.put(type, nodes);
					}
				}
				else if(type == AxiomDistributionType.CR_TYPE_BOTTOM && foundBottom) {
					nodes.add(availableNodes.get(i));
					i++;
					typeHostMap.put(type, nodes);
				}
			}
		}
		return i;
	}
	
	private void initialize() throws Exception {
		List<HostInfo> allHosts = new ArrayList<HostInfo>();
		Collection<List<HostInfo>> hostInfoCollection = typeHostMap.values();
		for(List<HostInfo> hinfoList : hostInfoCollection)
			allHosts.addAll(hinfoList);
		pipelineManager = new PipelineManager(allHosts, 
				propertyFileHandler.getPipelineQueueSize());
		Set<Entry<AxiomDistributionType,List<HostInfo>>> typeHostEntrySet = 
											typeHostMap.entrySet();	
		int i = 1;
		List<String> channels = new ArrayList<String>();
		String axiomTypeKey = propertyFileHandler.getAxiomTypeKey();
		String channelKey = propertyFileHandler.getChannelKey();
		double score = 1.0;
		for(Entry<AxiomDistributionType,List<HostInfo>> entry1 : 
			typeHostEntrySet) {
			// insert "TypeChannel" details (CR_TYPE1, "c1")
			for(HostInfo hostInfo : entry1.getValue()) {
				pipelineManager.pset(hostInfo, axiomTypeKey, 
						entry1.getKey().toString(), axiomDB);
				pipelineManager.pset(hostInfo, channelKey, "c"+i, axiomDB);
				if(!(entry1.getKey() == AxiomDistributionType.CONCEPT_ID) &&
				   !(entry1.getKey() == AxiomDistributionType.RESULT_TYPE)) 
				   channels.add("c"+i);
				for(Entry<AxiomDistributionType,List<HostInfo>> entry2 : 
					typeHostEntrySet) {
					score = 1.0;
					// insert type-host details on all nodes. AxiomType is the key
					for(HostInfo hostInfo2 : entry2.getValue())
						pipelineManager.pzadd(hostInfo, 
								entry2.getKey().toString(), score, 
								hostInfo2.toString(), axiomDB);
					score++;
				}
				i++;
			} 
		}
		
		// insert channel list on all hosts
		String allChannelsKey = propertyFileHandler.getAllChannelsKey();
		for(HostInfo host : allHosts)
			for(String channel : channels)
				pipelineManager.psadd(host, allChannelsKey, channel, 
						AxiomDB.NON_ROLE_DB, false);

		pipelineConjunctIndexManager = new PipelineManager(
						typeHostMap.get(AxiomDistributionType.CR_TYPE1_2), 
				propertyFileHandler.getPipelineQueueSize());
	}
	
	private void sortScoresAndAssignNodes(
			Map<AxiomDistributionType, Double> typeScoreMap, 
			List<HostInfo> availableNodes) {
		List<Entry<AxiomDistributionType, Double>> entrySet = 
			new ArrayList<Entry<AxiomDistributionType, Double>>(
					typeScoreMap.entrySet());
		Collections.sort(entrySet, Collections.reverseOrder(
				new ScoreComparator()));
		
		for(Entry<AxiomDistributionType, Double> entry : entrySet) {
//			System.out.println(entry.getKey().toString() + ":  " + entry.getValue());
			int numAssignedNodes = (int)Math.round(entry.getValue());
			if(!availableNodes.isEmpty()) {
				if(numAssignedNodes == 0) {
					// since its sorted, assign all remaining nodes to this type
					
					// assign only if this type has some axioms associated with it
					if(!typeAxiomsMap.get(entry.getKey()).isEmpty())
						typeHostMap.get(entry.getKey()).addAll(availableNodes);
					else {
						//no axioms of this type. So assign the remaining nodes
						//to the highest score type.
						typeHostMap.get(entrySet.get(0).getKey()).addAll(
								availableNodes);
					}
					availableNodes.clear();
				}
				else if(availableNodes.size() >= numAssignedNodes) {
					typeHostMap.get(entry.getKey()).addAll(
							availableNodes.subList(0, numAssignedNodes));
					availableNodes.subList(0, numAssignedNodes).clear();
				}
				else {
					//assign all remaining nodes to this type
					typeHostMap.get(entry.getKey()).addAll(availableNodes);
					availableNodes.clear();
				}
			}
			else
				break;
		}
	}
	
	private void computeScores(
			Map<AxiomDistributionType, Double> typeScoreMap, 
			List<AxiomDistributionType> axiomTypes, 
			Map<AxiomDistributionType, Double> typesWeightMap) {
		double score;
		//for some types, scores are computed specifically because
		//their count is unknown and can only be estimated or are given
		//special preference because they are rules related to roles.
		for(int i=0; i<numTypes; i++) {
			AxiomDistributionType type = axiomTypes.get(i);
			if(type == AxiomDistributionType.CR_TYPE3_2) {
				int type32AxiomCountEst = typeAxiomsMap.get(
						AxiomDistributionType.CR_TYPE2).size() + 
						typeAxiomsMap.get(
								AxiomDistributionType.CR_TYPE3_1).size() +  
						typeAxiomsMap.get(
								AxiomDistributionType.CR_TYPE4).size() + 
						typeAxiomsMap.get(
								AxiomDistributionType.CR_TYPE5).size();
				score = type32AxiomCountEst * typesWeightMap.get(type);
			}
			else if(type == AxiomDistributionType.CR_TYPE4 || 
					type == AxiomDistributionType.CR_TYPE5) 
				score = (typeAxiomsMap.get(
								AxiomDistributionType.CR_TYPE4).size() + 
						typeAxiomsMap.get(
								AxiomDistributionType.CR_TYPE5).size() + 
						typeAxiomsMap.get(
								AxiomDistributionType.CR_TYPE2).size()) * 
						typesWeightMap.get(type);
			else 
				score = typeAxiomsMap.get(type).size() * 
								typesWeightMap.get(type);
			typeScoreMap.put(type, score);
		}
	}
	
	private void categorizeAxiomsIntoTypes(
			OWLOntology normalizedOntology) throws Exception {
		// initialize the map
		for(AxiomDistributionType type : axiomTypes) 
			typeAxiomsMap.put(type, new HashSet<OWLLogicalAxiom>());
		Set<OWLSubClassOfAxiom> subClassAxioms = 
			normalizedOntology.getAxioms(AxiomType.SUBCLASS_OF);
		for(OWLSubClassOfAxiom axiom : subClassAxioms) {
			OWLClassExpression subClassExpression = axiom.getSubClass();
			OWLClassExpression superClassExpression = axiom.getSuperClass();
			if(isClass(subClassExpression)) {
				if (isExistential(superClassExpression)) {
					// A < 3r.B
					Set<OWLLogicalAxiom> type2Axioms = 
						typeAxiomsMap.get(AxiomDistributionType.CR_TYPE2);
					if(type2Axioms == null) {
						type2Axioms = new HashSet<OWLLogicalAxiom>();
						typeAxiomsMap.put(AxiomDistributionType.CR_TYPE2, 
								type2Axioms);
					}
					type2Axioms.add(axiom);
				}
				else if(isClass(superClassExpression)) {
					// A < B
					Set<OWLLogicalAxiom> type11Axioms = 
						typeAxiomsMap.get(AxiomDistributionType.CR_TYPE1_1);
					if(type11Axioms == null) {
						type11Axioms = new HashSet<OWLLogicalAxiom>();
						typeAxiomsMap.put(AxiomDistributionType.CR_TYPE1_1, 
								type11Axioms);
					}
					type11Axioms.add(axiom);
				}
			}
			else if(subClassExpression instanceof OWLObjectIntersectionOf) {
				// A1 ^ .... ^ An < B
				Set<OWLLogicalAxiom> type12Axioms = 
					typeAxiomsMap.get(AxiomDistributionType.CR_TYPE1_2);
				if(type12Axioms == null) {
					type12Axioms = new HashSet<OWLLogicalAxiom>();
					typeAxiomsMap.put(AxiomDistributionType.CR_TYPE1_2, 
							type12Axioms);
				}
				type12Axioms.add(axiom);
				if(!foundBottom) 
					if(superClassExpression.isBottomEntity())
						foundBottom = true;
			}
			else if(isExistential(subClassExpression)) {
				// 3r.A < B
				Set<OWLLogicalAxiom> type31Axioms = 
					typeAxiomsMap.get(AxiomDistributionType.CR_TYPE3_1);
				if(type31Axioms == null) {
					type31Axioms = new HashSet<OWLLogicalAxiom>();
					typeAxiomsMap.put(AxiomDistributionType.CR_TYPE3_1, 
							type31Axioms);
				}
				type31Axioms.add(axiom);
			}
			else
				throw new Exception("Unexpected SubClass type of axiom. Axiom: "
						+ subClassExpression.toString());
		}
		//convert role axioms into OWLLogicalAxiom type
		Set<OWLSubObjectPropertyOfAxiom> subObjPropertyAxioms = 
			normalizedOntology.getAxioms(AxiomType.SUB_OBJECT_PROPERTY);
		Set<OWLSubDataPropertyOfAxiom> subDataPropertyAxioms = 
			normalizedOntology.getAxioms(AxiomType.SUB_DATA_PROPERTY);
		Set<OWLLogicalAxiom> subPropertyAxioms = new HashSet<OWLLogicalAxiom>(
				subObjPropertyAxioms.size() + subDataPropertyAxioms.size());
		for(OWLSubObjectPropertyOfAxiom ax : subObjPropertyAxioms)
			subPropertyAxioms.add(ax);
		for(OWLSubDataPropertyOfAxiom ax : subDataPropertyAxioms)
			subPropertyAxioms.add(ax);
		typeAxiomsMap.put(AxiomDistributionType.CR_TYPE4, subPropertyAxioms);
		Set<OWLSubPropertyChainOfAxiom> roleChainAxioms = 
			normalizedOntology.getAxioms(AxiomType.SUB_PROPERTY_CHAIN_OF);
		Set<OWLLogicalAxiom> roleChainAxioms2 = 
			new HashSet<OWLLogicalAxiom>(roleChainAxioms.size());
		for(OWLSubPropertyChainOfAxiom ax : roleChainAxioms)
			roleChainAxioms2.add(ax);
		typeAxiomsMap.put(AxiomDistributionType.CR_TYPE5, roleChainAxioms2);
	}
	
	private boolean isClass(OWLClassExpression oce) {
		if(oce instanceof OWLClass || oce instanceof OWLIndividual ||
				oce instanceof OWLObjectOneOf)
			return true;
		else 
			return false;
	}
	
	private boolean isExistential(OWLClassExpression oce) {
		if(oce instanceof OWLObjectSomeValuesFrom || 
				oce instanceof OWLDataSomeValuesFrom || 
				oce instanceof OWLObjectHasValue || 
				oce instanceof OWLDataHasValue)
			return true;
		else
			return false;
	}
	
	private void loadNormalizedAxioms(OWLOntology normalizedOntology) throws Exception {
		
		String localKeys = propertyFileHandler.getLocalKeys();		
		Set<Entry<AxiomDistributionType, Set<OWLLogicalAxiom>>> 
				typeAxiomEntrySet = typeAxiomsMap.entrySet();
		for(Entry<AxiomDistributionType, Set<OWLLogicalAxiom>> entry : 
			typeAxiomEntrySet) {
			Set<OWLLogicalAxiom> axioms = entry.getValue();
			switch(entry.getKey()) {
				case CR_TYPE1_1: 
					// Axioms of type A < B
					insertType11Axioms(axioms, localKeys);
					break;
				case CR_TYPE1_2: 
					// This axiom is of type1. A1 ^ A2 ^ A3 ^ ...... ^ An -> B
					insertType12Axioms(axioms, localKeys);
					break;
				case CR_TYPE2: 
					// This axiom is of type2. A < 3r.B
					insertType2Axioms(axioms, localKeys, 
							normalizedOntology.getAxioms(
									AxiomType.DATA_PROPERTY_ASSERTION),
							normalizedOntology.getAxioms(
									AxiomType.OBJECT_PROPERTY_ASSERTION));
					insertPropertyDomainRangeAxioms(
							normalizedOntology.getAxioms(
									AxiomType.OBJECT_PROPERTY_DOMAIN), 
							normalizedOntology.getAxioms(
									AxiomType.OBJECT_PROPERTY_RANGE), 
							normalizedOntology.getAxioms(
									AxiomType.DATA_PROPERTY_DOMAIN));
					break;
				case CR_TYPE3_1: 
					// This axiom is of type3. 3r.A < B
					insertType31Axioms(axioms, localKeys);
					break;
				case CR_TYPE3_2:
					// There would be no axioms of this type
					break;
				case CR_TYPE4:
					// This axiom is of type4. r < s
					insertType4Axioms(axioms);
					break;
				case CR_TYPE5:
					// This axiom is of type5. r o s < t
					insertType5Axioms(axioms);
					break;
				case CR_TYPE_BOTTOM:
					if(!axioms.isEmpty())
						throw new Exception("Should be empty.");
					break;
				default:
					throw new Exception("Unknown Type: " + entry.getKey());
			}
		}
	}
	
	private void insertType31Axioms(Set<OWLLogicalAxiom> axioms, 
			String localKeys) throws Exception {
		if(axioms.isEmpty())
			return;
		List<HostInfo> type31HostInfoList = 
				getHostInfoList(AxiomDistributionType.CR_TYPE3_1);
		List<JedisShardInfo> type31ShardInfoList = 
			new ArrayList<JedisShardInfo>();
		for(HostInfo hostInfo : type31HostInfoList)
			type31ShardInfoList.add(new JedisShardInfo(hostInfo.getHost(), 
					hostInfo.getPort(), Constants.INFINITE_TIMEOUT));
		ShardedJedis type31ShardedJedis = new ShardedJedis(
										type31ShardInfoList, 
										Hashing.MURMUR_HASH);
		try {
			for(OWLLogicalAxiom axiom : axioms) {
				OWLSubClassOfAxiom subClassAxiom = (OWLSubClassOfAxiom)axiom;
				OWLClassExpression subClassExpression = 
					subClassAxiom.getSubClass();
				OWLClassExpression superClassExpression = 
					subClassAxiom.getSuperClass();
				String roleID = null;
				String valueID = null;
				if(subClassExpression instanceof OWLObjectSomeValuesFrom) {
					OWLObjectSomeValuesFrom osv = 
						(OWLObjectSomeValuesFrom)subClassExpression;
					roleID = conceptToID(osv.getProperty().toString());
					valueID = conceptToID(osv.getFiller().toString());
					String superClass = conceptToID(
							superClassExpression.toString());
					insertKV(valueID, roleID, superClass, type31ShardedJedis, 
							localKeys);
				}
				else if(subClassExpression instanceof OWLDataSomeValuesFrom) {
					OWLDataSomeValuesFrom osv = 
							(OWLDataSomeValuesFrom) subClassExpression;
					String propertyID = conceptToID(osv.getProperty().toString());
					if(osv.getFiller().isDatatype()) {
						String dataTypeID = conceptToIDForDataType(
								osv.getFiller().asOWLDatatype().toString());
						String superClass = conceptToID(
								superClassExpression.toString());
						insertKV(dataTypeID, propertyID, superClass, 
								type31ShardedJedis, localKeys);
					}
					else
						throw new Exception("Not a DataType... " + osv.getFiller());
				}
				else if(subClassExpression instanceof OWLObjectHasValue) {
					OWLObjectHasValue ohv = 
						(OWLObjectHasValue)subClassExpression;
					roleID = conceptToID(ohv.getProperty().toString());
					valueID = conceptToID(ohv.getValue().toString());
					String superClass = conceptToID(
							superClassExpression.toString());
					insertKV(valueID, roleID, superClass, 
							type31ShardedJedis, localKeys);
				}
				else if(subClassExpression instanceof OWLDataHasValue) {					
					OWLDataHasValue dsv = (OWLDataHasValue)subClassExpression;
					roleID = conceptToID(dsv.getProperty().toString());
					String dataTypeID = conceptToIDForDataType(
							dsv.getValue().getDatatype().toString());
					String superClass = conceptToID(
							superClassExpression.toString());
					insertKV(dataTypeID, roleID, superClass, 
							type31ShardedJedis, localKeys);
				}
				else
					throw new Exception("Unknown Type: " + subClassExpression);
			}
		}
		finally {
			type31ShardedJedis.disconnect();
		}
	}
	
	private void insertKV(String key, String propertyID, String fillerID, 
			ShardedJedis shardedJedis, String localKeys) throws Exception {
		JedisShardInfo shardInfo = 
			shardedJedis.getShardInfo(key);
		HostInfo shardHostInfo = new HostInfo(
				shardInfo.getHost(), 
				shardInfo.getPort());
		// key: A, value: Br
		pipelineManager.pzadd(shardHostInfo, localKeys, currentIncrement, 
				key, axiomDB);			
		StringBuilder superClassRole = 
			new StringBuilder(fillerID).append(propertyID);
		pipelineManager.pzadd(shardHostInfo, key, currentIncrement,
				superClassRole.toString(), axiomDB);
	}
	
	private void insertType2Axioms(Set<OWLLogicalAxiom> axioms, 
			String localKeys, 
			Set<OWLDataPropertyAssertionAxiom> dataPropAssertions, 
			Set<OWLObjectPropertyAssertionAxiom> objPropAssertions) 
	throws Exception {
		if(axioms.isEmpty() && dataPropAssertions.isEmpty())
			return;
		List<HostInfo> type2HostInfoList = 
				getHostInfoList(AxiomDistributionType.CR_TYPE2);
		List<JedisShardInfo> type2ShardInfoList = new ArrayList<JedisShardInfo>();
		for(HostInfo hostInfo : type2HostInfoList)
			type2ShardInfoList.add(new JedisShardInfo(hostInfo.getHost(), 
					hostInfo.getPort(), Constants.INFINITE_TIMEOUT));
		ShardedJedis type2ShardedJedis = new ShardedJedis(type2ShardInfoList, 
										Hashing.MURMUR_HASH);
		try {
			// adding ObjectPropertyAssertions to Type2 axioms
			for(OWLObjectPropertyAssertionAxiom opaxiom : objPropAssertions) 
				axioms.add(opaxiom.asOWLSubClassOfAxiom());
			for(OWLDataPropertyAssertionAxiom dpaxiom : dataPropAssertions)
				axioms.add(dpaxiom);
			
			for(OWLLogicalAxiom axiom : axioms) {
				OWLSubClassOfAxiom subClassAxiom = (OWLSubClassOfAxiom)axiom;
				OWLClassExpression subClassExpression = 
					subClassAxiom.getSubClass();
				OWLClassExpression superClassExpression = 
					subClassAxiom.getSuperClass();
				String key = conceptToID(subClassExpression.toString());
				JedisShardInfo shardInfo = 
					type2ShardedJedis.getShardInfo(key);
				HostInfo shardHostInfo = 
					new HostInfo(shardInfo.getHost(), 
								shardInfo.getPort());
				pipelineManager.pzadd(shardHostInfo, localKeys, 
						currentIncrement, key, axiomDB);
				String superClassExpressionID = null;
				if(superClassExpression instanceof OWLObjectSomeValuesFrom) {
					OWLObjectSomeValuesFrom osv = 
						(OWLObjectSomeValuesFrom)superClassExpression;
					superClassExpressionID = constructRoleFillerFromCE(
												osv.getProperty().toString(),
												osv.getFiller().toString() 
												);
					pipelineManager.pzadd(
							shardHostInfo, key, currentIncrement, 
							superClassExpressionID, axiomDB);
				}
				else if(superClassExpression instanceof OWLDataSomeValuesFrom) {
					OWLDataSomeValuesFrom dsv = 
								(OWLDataSomeValuesFrom) superClassExpression;
					String propertyID = conceptToID(dsv.getProperty().toString());
					// check DataType on RHS. Create ID and let it mix with A < 3 r.B
					if(dsv.getFiller().isDatatype()) {
						String dataTypeID = conceptToIDForDataType(
								dsv.getFiller().asOWLDatatype().toString());
						StringBuilder roleFiller = new StringBuilder(dataTypeID).
								append(propertyID);
						pipelineManager.pzadd(
								shardHostInfo, key, currentIncrement, 
								roleFiller.toString(), axiomDB);
					}
					else 
						throw new Exception("Not a datatype.. " + dsv.getFiller());
				}
				else if(superClassExpression instanceof OWLObjectHasValue) {
					OWLObjectHasValue ohv = 
						(OWLObjectHasValue)superClassExpression;
					superClassExpressionID = constructRoleFillerFromCE(
												ohv.getProperty().toString(),
												ohv.getValue().toString());
					pipelineManager.pzadd(
							shardHostInfo, key, currentIncrement, 
							superClassExpressionID, axiomDB);
				}
				else if(superClassExpression instanceof OWLDataHasValue) {
					OWLDataHasValue ohv = 
						(OWLDataHasValue)superClassExpression;
					String propertyID = conceptToID(ohv.getProperty().toString());
					String dataTypeID = conceptToIDForDataType(
									ohv.getValue().getDatatype().toString());
					StringBuilder roleFiller = new StringBuilder(dataTypeID).
							append(propertyID);
					pipelineManager.pzadd(
							shardHostInfo, key, currentIncrement, 
							roleFiller.toString(), axiomDB);
				}
				else
					throw new Exception("Unknown type: " + superClassExpression);
			}
		}
		finally {
			type2ShardedJedis.disconnect();
		}
	}
	
	private void insertPropertyDomainRangeAxioms(
			Set<OWLObjectPropertyDomainAxiom> objPropDomainAxioms, 
			Set<OWLObjectPropertyRangeAxiom> objPropRangeAxioms, 
			Set<OWLDataPropertyDomainAxiom> dataPropDomainAxioms) 
	throws Exception {
		if(objPropDomainAxioms.isEmpty() && objPropRangeAxioms.isEmpty() && 
				dataPropDomainAxioms.isEmpty())
			return;
		List<HostInfo> type2HostInfoList = 
				getHostInfoList(AxiomDistributionType.CR_TYPE2);
		List<JedisShardInfo> type2ShardInfoList = new ArrayList<JedisShardInfo>();
		for(HostInfo hostInfo : type2HostInfoList)
			type2ShardInfoList.add(new JedisShardInfo(hostInfo.getHost(), 
					hostInfo.getPort(), Constants.INFINITE_TIMEOUT));
		ShardedJedis type2ShardedJedis = new ShardedJedis(type2ShardInfoList, 
										Hashing.MURMUR_HASH);
		PipelineManager type2PipelineManager = new PipelineManager(
				type2HostInfoList, propertyFileHandler.getPipelineQueueSize());
		AxiomDB propDomainRangeDB = AxiomDB.DB5;
		try {
			for(OWLObjectPropertyDomainAxiom ax : objPropDomainAxioms) {
				String propID = conceptToID(ax.getProperty().toString());
				String domainID = conceptToID(ax.getDomain().toString());
				JedisShardInfo shardInfo = type2ShardedJedis.getShardInfo(propID);
				HostInfo hostInfo = new HostInfo(shardInfo.getHost(), 
						shardInfo.getPort());
				type2PipelineManager.phset(hostInfo, propID, 
						Constants.DOMAIN_FIELD, domainID, propDomainRangeDB);
			}
			for(OWLObjectPropertyRangeAxiom ax : objPropRangeAxioms) {
				if(ax.getRange() instanceof OWLObjectUnionOf)
					continue;
				String propID = conceptToID(ax.getProperty().toString());
				String rangeID = conceptToID(ax.getRange().toString());
				JedisShardInfo shardInfo = type2ShardedJedis.getShardInfo(propID);
				HostInfo hostInfo = new HostInfo(shardInfo.getHost(), 
						shardInfo.getPort());
				type2PipelineManager.phset(hostInfo, propID, 
						Constants.RANGE_FIELD, rangeID, propDomainRangeDB);
			}
			for(OWLDataPropertyDomainAxiom ax : dataPropDomainAxioms) {
				String propID = conceptToID(ax.getProperty().toString());
				String domainID = conceptToID(ax.getDomain().toString());
				JedisShardInfo shardInfo = type2ShardedJedis.getShardInfo(propID);
				HostInfo hostInfo = new HostInfo(shardInfo.getHost(), 
						shardInfo.getPort());
				type2PipelineManager.phset(hostInfo, propID, 
						Constants.DOMAIN_FIELD, domainID, propDomainRangeDB);
			}
			type2PipelineManager.synchAll(propDomainRangeDB);
		}
		finally {
			type2ShardedJedis.disconnect();
			type2PipelineManager.closeAll();
		}
	}
	
	private void insertType12Axioms(Set<OWLLogicalAxiom> axioms, 
			String localKeys) throws Exception {
		if(axioms.isEmpty())
			return;
		List<HostInfo> type12HostInfoList = getHostInfoList(
				AxiomDistributionType.CR_TYPE1_2);
		List<JedisShardInfo> type12ShardInfoList = new ArrayList<JedisShardInfo>();
		for(HostInfo hostInfo : type12HostInfoList)
			type12ShardInfoList.add(new JedisShardInfo(hostInfo.getHost(), 
				hostInfo.getPort(), Constants.INFINITE_TIMEOUT));
		ShardedJedis type12ShardedJedis = new ShardedJedis(type12ShardInfoList, 
									Hashing.MURMUR_HASH);
		try {
			for(OWLLogicalAxiom axiom : axioms) {
				OWLSubClassOfAxiom subClassAxiom = (OWLSubClassOfAxiom)axiom; 
				OWLClassExpression subClassExpression = 
					subClassAxiom.getSubClass();
				OWLClassExpression superClassExpression = 
					subClassAxiom.getSuperClass();
				OWLObjectIntersectionOf intersectionAxiom = 
					(OWLObjectIntersectionOf) subClassExpression;
				Set<OWLClassExpression> operands = 
					intersectionAxiom.getOperands();
				StringBuilder operandIDs = new StringBuilder();
				Set<String> operandIDSet = new HashSet<String>();
				for(OWLClassExpression op : operands) {
					String opStr = conceptToID(op.toString());
					operandIDSet.add(opStr);
					operandIDs.append(opStr);
				}		
				
				// use DB-3. for each operand, insert (op, operandIDs) as (key,value)
				// splitting up the conjunct indices across the Type1 shards
				for(String op : operandIDSet) {
					JedisShardInfo opShardInfo = 
						type12ShardedJedis.getShardInfo(op);
					HostInfo shardHostInfo = new HostInfo(
							opShardInfo.getHost(), 
							opShardInfo.getPort());
					pipelineConjunctIndexManager.psadd(
							shardHostInfo, op, operandIDs.toString(), 
							AxiomDB.CONJUNCT_INDEX_DB, false);
					
					pipelineManager.pzadd(shardHostInfo, localKeys,  
							currentIncrement, operandIDs.toString(), axiomDB);
					pipelineManager.pzadd(shardHostInfo, 
							operandIDs.toString(), currentIncrement,  
							conceptToID(
									superClassExpression.toString()), 
									axiomDB);
				}
				operandIDSet.clear();
			}
		}
		finally {
			type12ShardedJedis.disconnect();
		}
	}
	
	private void insertType11Axioms(Set<OWLLogicalAxiom> axioms, 
			String localKeys) throws Exception {
		if(axioms.isEmpty())
			return;
		List<HostInfo> type1HostInfoList = 
				getHostInfoList(AxiomDistributionType.CR_TYPE1_1);
		List<JedisShardInfo> type1ShardInfoList = new ArrayList<JedisShardInfo>();
		for(HostInfo hostInfo : type1HostInfoList)
			type1ShardInfoList.add(new JedisShardInfo(hostInfo.getHost(), 
					hostInfo.getPort(), Constants.INFINITE_TIMEOUT));
		ShardedJedis type1ShardedJedis = new ShardedJedis(type1ShardInfoList, 
										Hashing.MURMUR_HASH);
		try {
			for(OWLLogicalAxiom axiom : axioms) {
				OWLSubClassOfAxiom subClassAxiom = (OWLSubClassOfAxiom)axiom;
				OWLClassExpression subClassExpression = 
					subClassAxiom.getSubClass();
				OWLClassExpression superClassExpression = 
					subClassAxiom.getSuperClass();
				String key = null;
				if(subClassExpression instanceof OWLObjectOneOf) {
					OWLObjectOneOf individual = 
						(OWLObjectOneOf)subClassExpression;
					Set<OWLIndividual> individuals = 
						individual.getIndividuals();
					String superID = conceptToID(superClassExpression.toString());
					for(OWLIndividual ind : individuals) {
						key = conceptToID(ind.toString());
						JedisShardInfo shardInfo = 
							type1ShardedJedis.getShardInfo(key);
						HostInfo shardHostInfo = new HostInfo(
								shardInfo.getHost(), 
								shardInfo.getPort());
						pipelineManager.pzadd(shardHostInfo, localKeys, 
								currentIncrement, key, axiomDB);
						pipelineManager.pzadd(
								new HostInfo(shardInfo.getHost(), 
										shardInfo.getPort()), 
								key, currentIncrement, superID, axiomDB);
					}
				}
				else
					key = conceptToID(
							subClassExpression.toString());
				if(superClassExpression instanceof OWLObjectOneOf) {
					OWLObjectOneOf superIndividual = 
						(OWLObjectOneOf) superClassExpression;
					Set<OWLIndividual> individuals = 
						superIndividual.getIndividuals();
					if(individuals.size() > 1) {
//						System.out.println("Skipping ObjectOneOf - has many " +
//								"individuals on RHS");
						continue;
					}
					JedisShardInfo shardInfo = 
						type1ShardedJedis.getShardInfo(key);
					HostInfo shardHostInfo = new HostInfo(
							shardInfo.getHost(), 
							shardInfo.getPort());
					pipelineManager.pzadd(shardHostInfo, localKeys, 
							currentIncrement, key, axiomDB);
					
					for(OWLIndividual ind : individuals) {
						pipelineManager.pzadd(
								new HostInfo(shardInfo.getHost(), 
										shardInfo.getPort()), 
								key, currentIncrement, 
								conceptToID(ind.toString()), axiomDB);
					}
				}
				else {
					JedisShardInfo shardInfo = 
						type1ShardedJedis.getShardInfo(key);
					HostInfo shardHostInfo = new HostInfo(
							shardInfo.getHost(), 
							shardInfo.getPort());
					pipelineManager.pzadd(shardHostInfo, localKeys, 
							currentIncrement, key, axiomDB);
					pipelineManager.pzadd(
							new HostInfo(shardInfo.getHost(), 
									shardInfo.getPort()), 
							key, currentIncrement, conceptToID(
											superClassExpression.toString()), 
							axiomDB);
				}
			}
		}
		finally {
			type1ShardedJedis.disconnect();
		}
	}
	
	private void insertType4Axioms(Set<OWLLogicalAxiom> axioms) 
	throws Exception {
		if(axioms.isEmpty())
			return;
		List<HostInfo> type4HostInfoList = 
				getHostInfoList(AxiomDistributionType.CR_TYPE4);
		List<JedisShardInfo> type4Shards = new ArrayList<JedisShardInfo>();
		for(HostInfo hinfo : type4HostInfoList) {
			type4Shards.add(new JedisShardInfo(hinfo.getHost(), 
					hinfo.getPort(), Constants.INFINITE_TIMEOUT));
		}
		ShardedJedis type4ShardedJedis = new ShardedJedis(type4Shards, 
											Hashing.MURMUR_HASH);
		try {
			// for type4, for each r < s, do (r,s) in db-0
			for(OWLLogicalAxiom ax : axioms) {
				OWLSubObjectPropertyOfAxiom subPropertyAxiom = 
					(OWLSubObjectPropertyOfAxiom)ax;
				String key = conceptToID(
						subPropertyAxiom.getSubProperty().toString());
				String superPropKey = conceptToID(
						subPropertyAxiom.getSuperProperty().toString());
				JedisShardInfo shardInfo = type4ShardedJedis.getShardInfo(key);
				pipelineManager.psadd(
						new HostInfo(shardInfo.getHost(), shardInfo.getPort()), 
						key, superPropKey, axiomDB, false);
			}
		}
		finally {
			type4ShardedJedis.disconnect();
		}
	}
	
	private void insertType5Axioms(Set<OWLLogicalAxiom> axioms) 
	throws Exception {
		if(axioms.isEmpty())
			return;
		List<HostInfo> type5HostInfoList = 
				getHostInfoList(AxiomDistributionType.CR_TYPE5);
		List<JedisShardInfo> type5Shards = new ArrayList<JedisShardInfo>();
		for(HostInfo hinfo : type5HostInfoList) {
			type5Shards.add(new JedisShardInfo(hinfo.getHost(), 
					hinfo.getPort(), Constants.INFINITE_TIMEOUT));
		}
		ShardedJedis type5ShardedJedis = new ShardedJedis(type5Shards, 
											Hashing.MURMUR_HASH);
		try {
			// for type5, for each r o s < t, do (r,st) in db-0 and (s,rt) in db-1
			for(OWLLogicalAxiom ax : axioms) {
				OWLSubPropertyChainOfAxiom roleChainAxiom = 
					(OWLSubPropertyChainOfAxiom)ax;
				List<OWLObjectPropertyExpression> propertyChain = 
					roleChainAxiom.getPropertyChain();
				List<String> chainIDList = new ArrayList<String>();
				for(OWLObjectPropertyExpression pe : propertyChain) {
					String id = conceptToID(pe.toString());
					chainIDList.add(id);
				}
				if(chainIDList.size() != 2)
					throw new Exception("Role Chain greater than 2: " + 
							propertyChain.size());
				String superPropKey = conceptToID(
						roleChainAxiom.getSuperProperty().toString());
				StringBuilder value1 = new StringBuilder(chainIDList.get(1)).
					append(superPropKey);
				StringBuilder value2 = new StringBuilder(chainIDList.get(0)).
					append(superPropKey);
				JedisShardInfo shardInfo = 
					type5ShardedJedis.getShardInfo(chainIDList.get(0));
				pipelineManager.psadd(
						new HostInfo(shardInfo.getHost(), shardInfo.getPort()), 
						chainIDList.get(0), value1.toString(), axiomDB, false);
				Jedis shard = type5ShardedJedis.getShard(chainIDList.get(1));
				shard.select(1);
				shard.sadd(chainIDList.get(1), value2.toString());
				shard.select(0);	//reset it back to 0
			}
		}
		finally {
			type5ShardedJedis.disconnect();
		}
	}
	
	/**
	 * typeHost map is not used and instead this method should be used for
	 * internal use primarily to retain the order of populating hosts in the 
	 * list. If the order varies, sharding keys destination changes. If read 
	 * from DB everytime, the order should be the same.
	 * @param axiomType
	 * @return
	 */
	private List<HostInfo> getHostInfoList(AxiomDistributionType axiomType) {
		Set<String> hosts = idReader.zrange(axiomType.toString(), 
				Constants.RANGE_BEGIN, Constants.RANGE_END);
		List<HostInfo> hostInfoList = new ArrayList<HostInfo>(hosts.size());
		for(String host : hosts) {
			String[] hostPort = host.split(":");
			HostInfo hostInfo = new HostInfo(hostPort[0], 
					Integer.parseInt(hostPort[1]));
			hostInfoList.add(hostInfo);
		}
		return hostInfoList;
	}
	
	private void mapConceptToID(OWLOntology ontology) throws Exception {
		Set<OWLClass> ontologyClasses = ontology.getClassesInSignature(); 
		Set<OWLObjectProperty> ontologyProperties = 
			ontology.getObjectPropertiesInSignature();
		String topConcept = ontology.getOWLOntologyManager().
							getOWLDataFactory().getOWLThing().
							toString();
		String bottomConcept = ontology.getOWLOntologyManager().
								getOWLDataFactory().
								getOWLNothing().toString();		
		HostInfo idNodeInfo = typeHostMap.get(
				AxiomDistributionType.CONCEPT_ID).get(0);
		HostInfo resultNodeInfo = typeHostMap.get(
				AxiomDistributionType.RESULT_TYPE).get(0);
//		long conceptRoleCount = 1;
		String lastCountKey = "lastCount";
		String lastCountStr = idReader.get(lastCountKey);
		String topConceptID;
		String bottomConceptID = Util.getPackedID(Constants.BOTTOM_ID, 
				EntityType.CLASS);
		if(lastCountStr != null) {
			conceptRoleCount = Long.parseLong(lastCountStr);
			topConceptID = idReader.get(topConcept);
		}
		else {
			topConceptID = Util.getPackedID(Constants.TOP_ID, EntityType.CLASS); 
						
			// add top & bottom concepts
			pipelineManager.pset(idNodeInfo, topConcept, topConceptID, axiomDB);
			pipelineManager.pset(idNodeInfo, topConceptID, topConcept, axiomDB);
			pipelineManager.pzadd(resultNodeInfo, topConceptID, 
					Constants.INIT_SCORE, topConceptID, axiomDB);
			
			pipelineManager.pset(idNodeInfo, bottomConcept, 
					bottomConceptID, axiomDB);
			pipelineManager.pset(idNodeInfo, bottomConceptID, 
					bottomConcept, axiomDB);
			pipelineManager.pzadd(resultNodeInfo, bottomConceptID, 
					Constants.INIT_SCORE, bottomConceptID, axiomDB);
		}
		
		PipelineManager pipelineIDReader = new PipelineManager(
				Collections.singletonList(idNodeInfo), 
				propertyFileHandler.getPipelineQueueSize());
		
		//check if these classes have already been assigned IDs
		for(OWLClass cl : ontologyClasses) {
			pipelineIDReader.pget(idNodeInfo, cl.toString(), axiomDB);
		}
		pipelineIDReader.selectiveSynch(axiomDB, idNodeInfo);
		List<Response<String>> conceptIDs = pipelineIDReader.getResponseList();
		
		int foundBottomID = 0;
		if(foundBottom)
			foundBottomID = 1;
		int i = 0;
		for(OWLClass cl : ontologyClasses) {
			String concept = cl.toString();
			if(conceptIDs.get(i).get() != null) {
				i++;
				//id for this concept already exists
				continue;
			}
			i++;
			if(concept.equals(Constants.TOP))
				continue;
			else if(concept.equals(bottomConcept)) {
				foundBottom = true;
				foundBottomID = 1;
				continue;
			}

			//TOP_ID is 1
			
			conceptRoleCount++;
			String conceptID = Util.getPackedID(conceptRoleCount, 
									EntityType.CLASS); 
			pipelineManager.pset(idNodeInfo, concept, conceptID, axiomDB);
			pipelineManager.pset(idNodeInfo, conceptID, concept, axiomDB);
			conceptIDCache.put(concept, conceptID);
			
			// initialize S(X) = {X,T} in the result node
			// due to storage pattern, it is X = {X} and T = {X}
			// and S(bottom) = {all classes}
			pipelineManager.pzadd(resultNodeInfo, conceptID, 
					Constants.INIT_SCORE, conceptID, axiomDB);
			pipelineManager.pzadd(resultNodeInfo, conceptID, 
					Constants.INIT_SCORE, bottomConceptID, axiomDB);
			pipelineManager.pzadd(resultNodeInfo, topConceptID, 
					Constants.INIT_SCORE, conceptID, axiomDB);
		}
		conceptIDs.clear();
		
		//Old: insert foundBottom in all type2, type4, type5 and ID nodes 
		//Now: Inserting on all nodes since any node can process any rule
		//		due to work stealing.
		List<HostInfo> bottomHosts = new ArrayList<HostInfo>();
		Collection<List<HostInfo>> hostInfoCollection = typeHostMap.values();
		for(List<HostInfo> hinfoList : hostInfoCollection)
			bottomHosts.addAll(hinfoList);
		for(HostInfo hostInfo : bottomHosts)
			pipelineManager.pset(hostInfo, Constants.FOUND_BOTTOM, 
					Integer.toString(foundBottomID), axiomDB);
		
		// add individuals
		Set<OWLNamedIndividual> individuals = 
			ontology.getIndividualsInSignature();
		for(OWLNamedIndividual ind : individuals) {
			pipelineIDReader.pget(idNodeInfo, ind.toString(), axiomDB);
		}
		pipelineIDReader.selectiveSynch(axiomDB, idNodeInfo);
		List<Response<String>> individualIDs = pipelineIDReader.getResponseList();
		i = 0;
		for(OWLNamedIndividual individual : individuals) {
			if(individualIDs.get(i).get() != null) {
				i++;
				continue;
			}
			String individualStr = individual.toString();
			conceptRoleCount++;
			String id = Util.getPackedID(conceptRoleCount, EntityType.INDIVIDUAL);
			pipelineManager.pset(idNodeInfo, individualStr, id, axiomDB);
			pipelineManager.pset(idNodeInfo, id, individualStr, axiomDB);
			conceptIDCache.put(individualStr, id);
			
			// initialize S({x}) = {{x},T} in the result node
			// due to storage pattern, it is X = {X} and T = {X}
			// and S(bottom) = {all classes}
			pipelineManager.pzadd(resultNodeInfo, id, 
					Constants.INIT_SCORE, id, axiomDB);
			pipelineManager.pzadd(resultNodeInfo, id, 
					Constants.INIT_SCORE, bottomConceptID, axiomDB);
			pipelineManager.pzadd(resultNodeInfo, topConceptID, 
					Constants.INIT_SCORE, id, axiomDB);			
			i++;
		}
		individualIDs.clear();
		
		// add roles
		for(OWLObjectProperty property : ontologyProperties) {
			pipelineIDReader.pget(idNodeInfo, property.toString(), axiomDB);
		}
		pipelineIDReader.selectiveSynch(axiomDB, idNodeInfo);
		List<Response<String>> propertyIDs = pipelineIDReader.getResponseList();
		i = 0;
		for(OWLObjectProperty property : ontologyProperties) {
			if(propertyIDs.get(i).get() != null) {
				i++;
				continue;
			}
			String role = property.toString();
			conceptRoleCount++;
			String roleID = Util.getPackedID(conceptRoleCount, EntityType.ROLE); 
			pipelineManager.pset(idNodeInfo, role, roleID, axiomDB);
			pipelineManager.pset(idNodeInfo, roleID, role, axiomDB);
			conceptIDCache.put(role, roleID);
			i++;
		}
		propertyIDs.clear();
		Set<OWLDataProperty> dataProperties = 
			ontology.getDataPropertiesInSignature();
		for(OWLDataProperty property : dataProperties) {
			pipelineIDReader.pget(idNodeInfo, property.toString(), axiomDB);
		}
		pipelineIDReader.selectiveSynch(axiomDB, idNodeInfo);
		propertyIDs = pipelineIDReader.getResponseList();
		i = 0;
		for(OWLDataProperty property : dataProperties) {
			if(propertyIDs.get(i).get() != null) {
				i++;
				continue;
			}
			String role = property.toString();
			conceptRoleCount++;
			String roleID = Util.getPackedID(conceptRoleCount, EntityType.ROLE); 
			pipelineManager.pset(idNodeInfo, role, roleID, axiomDB);
			pipelineManager.pset(idNodeInfo, roleID, role, axiomDB);
			conceptIDCache.put(role, roleID);
			i++;
		}
		propertyIDs.clear();
		pipelineIDReader.closeAll();
		conceptRoleCount++;
		pipelineManager.pset(idNodeInfo, lastCountKey, 
				Long.toString(conceptRoleCount), axiomDB);
	}
	
	private String conceptToID(String concept) throws Exception {
		String conceptID = conceptIDCache.get(concept);
		if(conceptID == null || conceptID.isEmpty()) {
			conceptID = idReader.get(concept);
			if(conceptID == null)
				throw new Exception("Cannot find the concept in DB: " + 
						concept);
			else
				conceptIDCache.put(concept, conceptID);
		}
		return conceptID;
	}
	
	private String conceptToIDForDataType(String dataType) throws Exception {
		String dataTypeID = conceptIDCache.get(dataType);
		if(dataTypeID == null || dataTypeID.isEmpty()) {
			dataTypeID = idReader.get(dataType);
			if(dataTypeID == null) {
				// this datatype is not in DB. So insert it.
				conceptRoleCount++;
				dataTypeID = Util.getPackedID(conceptRoleCount, 
						EntityType.DATATYPE);
				idReader.set(dataType, dataTypeID);
				idReader.set(dataTypeID, dataType);
			}
			conceptIDCache.put(dataType, dataTypeID);
		}
		return dataTypeID;
	}
	
	private String constructRoleFillerFromCE(String property, String value) 
				throws Exception {
		StringBuilder roleFiller = new StringBuilder(conceptToID(value)).
									append(conceptToID(property));
		return roleFiller.toString();
	}
	
	public static void main(String[] args) throws Exception {
		if(args.length != 4) {
			System.out.println("Expecting 4 arguments -- Ontology path, " +
					"isNormalized(true/false), isIncrementalData(true/false) " +
					"and isTripsData(true/false)");
			System.exit(-1);
		}
		GregorianCalendar start = new GregorianCalendar();
		AxiomLoader axiomLoader = new AxiomLoader(args[0], Boolean.parseBoolean(args[1]), 
				Boolean.parseBoolean(args[2]), Boolean.parseBoolean(args[3]));
		axiomLoader.loadAxioms();
		System.out.println("Time taken (millis): " + 
				Util.getElapsedTime(start));
	}
}



