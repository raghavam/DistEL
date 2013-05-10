package knoelab.classification.init;

import java.io.File;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import knoelab.classification.misc.AxiomDB;
import knoelab.classification.misc.Constants;
import knoelab.classification.misc.HostInfo;
import knoelab.classification.misc.PropertyFileHandler;
import knoelab.classification.pipeline.PipelineManager;

import org.semanticweb.owlapi.apibinding.OWLManager;
import org.semanticweb.owlapi.io.RDFXMLOntologyFormat;
import org.semanticweb.owlapi.model.AxiomType;
import org.semanticweb.owlapi.model.IRI;
import org.semanticweb.owlapi.model.OWLClass;
import org.semanticweb.owlapi.model.OWLClassExpression;
import org.semanticweb.owlapi.model.OWLObjectIntersectionOf;
import org.semanticweb.owlapi.model.OWLObjectProperty;
import org.semanticweb.owlapi.model.OWLObjectPropertyExpression;
import org.semanticweb.owlapi.model.OWLObjectSomeValuesFrom;
import org.semanticweb.owlapi.model.OWLOntology;
import org.semanticweb.owlapi.model.OWLOntologyManager;
import org.semanticweb.owlapi.model.OWLOntologyStorageException;
import org.semanticweb.owlapi.model.OWLSubClassOfAxiom;
import org.semanticweb.owlapi.model.OWLSubObjectPropertyOfAxiom;
import org.semanticweb.owlapi.model.OWLSubPropertyChainOfAxiom;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisShardInfo;
import redis.clients.jedis.ShardedJedis;
import redis.clients.util.Hashing;

/**
 * Splits the axioms based on their type and 
 * assigns the axioms to a node in the cluster. 
 * 
 * @author Raghava
 *
 */

// TODO: If axioms of a particular type are more, split them up - load balancing
// TODO: Can initialization code S(X) = {X} be multi-threaded (or separate process)?
public class AxiomLoader {
	
	private Map<AxiomDistributionType, List<HostInfo>> typeHostMap;
	private PipelineManager pipelineManager;
	private PipelineManager pipelineConjunctIndexManager;
	private PropertyFileHandler propertyFileHandler;
	private Jedis idReader;
	private AxiomDB axiomDB;
	
	public AxiomLoader() throws Exception {
		propertyFileHandler = PropertyFileHandler.getInstance();
		List<HostInfo> hostInfoList = propertyFileHandler.getAllHostsInfo();
		if(hostInfoList.size() < 5)
			throw new Exception(
					"Cluster size should be at least 5. Current size: " + 
					hostInfoList.size());
		pipelineManager = new PipelineManager(hostInfoList, 
				propertyFileHandler.getPipelineQueueSize());
		// this class uses only the non-role db (0)
		axiomDB = AxiomDB.NON_ROLE_DB;
		typeHostMap = propertyFileHandler.getTypeHostInfo();
		Set<Entry<AxiomDistributionType,List<HostInfo>>> typeHostEntrySet = 
											typeHostMap.entrySet();	
		int i = 1;
		List<String> channels = new ArrayList<String>();
		String axiomTypeKey = propertyFileHandler.getAxiomTypeKey();
		String channelKey = propertyFileHandler.getChannelKey();
		for(Entry<AxiomDistributionType,List<HostInfo>> entry1 : typeHostEntrySet) {
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
					// insert type-host details on all nodes. AxiomType is the key
					for(HostInfo hostInfo2 : entry2.getValue())
						pipelineManager.psadd(hostInfo, 
								entry2.getKey().toString(), 
								hostInfo2.toString(), axiomDB, false);
				}
				i++;
			} 
		}
		
		// insert channel list on all hosts
		List<HostInfo> allHosts = propertyFileHandler.getAllHostsInfo();
		String allChannelsKey = propertyFileHandler.getAllChannelsKey();
		for(HostInfo host : allHosts)
			for(String channel : channels)
				pipelineManager.psadd(host, allChannelsKey, channel, 
						AxiomDB.NON_ROLE_DB, false);

		List<HostInfo> idHostInfoList = typeHostMap.get(AxiomDistributionType.CONCEPT_ID);
		// Concept IDs are always on 1 machine
		idReader = new Jedis(idHostInfoList.get(0).getHost(), 
				idHostInfoList.get(0).getPort(), Constants.INFINITE_TIMEOUT);
		pipelineConjunctIndexManager = new PipelineManager(
						typeHostMap.get(AxiomDistributionType.CR_TYPE1_2), 
				propertyFileHandler.getPipelineQueueSize());
	}
	
	public void loadAxioms(String ontoFile, String isNormalizedStr) throws Exception {		
		OWLOntologyManager manager = OWLManager.createOWLOntologyManager();
        File owlFile = new File(ontoFile);
        IRI documentIRI = IRI.create(owlFile);
        OWLOntology ontology = manager.loadOntologyFromOntologyDocument(documentIRI);
        boolean isNormalized = Boolean.parseBoolean(isNormalizedStr);
        OWLOntology normalizedOntology = ontology;
        if(!isNormalized) {
        	Normalizer normalizer = new Normalizer(manager, ontology);
        	normalizedOntology = normalizer.Normalize();
        }
        mapConceptToID(normalizedOntology);
        // Results and Concept IDs are only on 1 machine
        pipelineManager.selectiveSynch(axiomDB, 
        		typeHostMap.get(AxiomDistributionType.CONCEPT_ID).get(0), 
        		typeHostMap.get(AxiomDistributionType.RESULT_TYPE).get(0));
        loadNormalizedAxioms(normalizedOntology);	
        
       	// TODO: remove this after testing
        if(!isNormalized) {
        	saveNormalizedOntology(owlFile, manager, normalizedOntology);
        	System.out.println("Finished saving normalized ontology");
        }
        
        // close all DB connections
        idReader.disconnect();
        pipelineManager.synchAndCloseAll(axiomDB);
        pipelineConjunctIndexManager.synchAndCloseAll(AxiomDB.CONJUNCT_INDEX_DB);
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
	
	private void loadNormalizedAxioms(OWLOntology normalizedOntology) throws Exception {
		
		String localKeys = propertyFileHandler.getLocalKeys();
		List<HostInfo> type2HostInfoList = 
			typeHostMap.get(AxiomDistributionType.CR_TYPE2);
		List<JedisShardInfo> type2ShardInfoList = new ArrayList<JedisShardInfo>();
		for(HostInfo hostInfo : type2HostInfoList)
			type2ShardInfoList.add(new JedisShardInfo(hostInfo.getHost(), 
					hostInfo.getPort(), Constants.INFINITE_TIMEOUT));
		ShardedJedis type2ShardedJedis = new ShardedJedis(type2ShardInfoList, 
										Hashing.MURMUR_HASH);
		
		List<HostInfo> type1HostInfoList = 
			typeHostMap.get(AxiomDistributionType.CR_TYPE1_1);
		List<JedisShardInfo> type1ShardInfoList = new ArrayList<JedisShardInfo>();
		for(HostInfo hostInfo : type1HostInfoList)
			type1ShardInfoList.add(new JedisShardInfo(hostInfo.getHost(), 
					hostInfo.getPort(), Constants.INFINITE_TIMEOUT));
		ShardedJedis type1ShardedJedis = new ShardedJedis(type1ShardInfoList, 
										Hashing.MURMUR_HASH);
		
		List<HostInfo> type12HostInfoList = 
				typeHostMap.get(AxiomDistributionType.CR_TYPE1_2);
		List<JedisShardInfo> type12ShardInfoList = new ArrayList<JedisShardInfo>();
		for(HostInfo hostInfo : type12HostInfoList)
			type12ShardInfoList.add(new JedisShardInfo(hostInfo.getHost(), 
					hostInfo.getPort(), Constants.INFINITE_TIMEOUT));
		ShardedJedis type12ShardedJedis = new ShardedJedis(type12ShardInfoList, 
										Hashing.MURMUR_HASH);
		
		List<HostInfo> type31HostInfoList = 
			typeHostMap.get(AxiomDistributionType.CR_TYPE3_1);
		List<JedisShardInfo> type31ShardInfoList = new ArrayList<JedisShardInfo>();
		for(HostInfo hostInfo : type31HostInfoList)
			type31ShardInfoList.add(new JedisShardInfo(hostInfo.getHost(), 
					hostInfo.getPort(), Constants.INFINITE_TIMEOUT));
		ShardedJedis type31ShardedJedis = new ShardedJedis(type31ShardInfoList, 
										Hashing.MURMUR_HASH);
		
		// Subclass axioms are of 3 types -- A ^ B < D, A < 3r.B, 3r.A < B 
		Set<OWLSubClassOfAxiom> subClassAxioms = normalizedOntology.getAxioms(AxiomType.SUBCLASS_OF);
		for(OWLSubClassOfAxiom subClassAxiom : subClassAxioms) {
			OWLClassExpression subClassExpression = subClassAxiom.getSubClass();
			OWLClassExpression superClassExpression = subClassAxiom.getSuperClass();
			if (subClassExpression instanceof OWLClass) {
				if (superClassExpression instanceof OWLObjectSomeValuesFrom) {
					// This axiom is of type2. A < 3r.B
					String key = conceptToID(subClassExpression.toString());
					JedisShardInfo shardInfo = type2ShardedJedis.getShardInfo(key);
					HostInfo shardHostInfo = new HostInfo(shardInfo.getHost(), 
													shardInfo.getPort());
					pipelineManager.psadd(shardHostInfo, localKeys, key, 
										axiomDB, false);
					pipelineManager.psadd(
						shardHostInfo, 
						key, constructRoleFillerFromCE(
								(OWLObjectSomeValuesFrom)superClassExpression),
						axiomDB, false);
				}
				else if (superClassExpression instanceof OWLClass) {
					// This axiom is of type1. A < B
					String key = conceptToID(subClassExpression.toString());
					JedisShardInfo shardInfo = type1ShardedJedis.getShardInfo(key);
					HostInfo shardHostInfo = new HostInfo(shardInfo.getHost(), 
							shardInfo.getPort());
					pipelineManager.psadd(shardHostInfo, localKeys, key, 
							axiomDB, false);
					pipelineManager.psadd(
							new HostInfo(shardInfo.getHost(), shardInfo.getPort()), 
							key, conceptToID(superClassExpression.toString()), 
							axiomDB, false);
				}
				else
					throw new Exception("Unexpected SuperClass type of axiom. Axiom: "
									+ superClassExpression.toString());
			}
			else if (subClassExpression instanceof OWLObjectIntersectionOf) {
				// This axiom is of type1. A1 ^ A2 ^ A3 ^ ...... ^ An -> B
				OWLObjectIntersectionOf intersectionAxiom = (OWLObjectIntersectionOf) subClassExpression;
				Set<OWLClassExpression> operands = intersectionAxiom.getOperands();
				StringBuilder operandIDs = new StringBuilder();
				Set<String> operandIDSet = new HashSet<String>();
				for(OWLClassExpression op : operands) {
					String opStr = conceptToID(op.toString());
					operandIDSet.add(opStr);
					operandIDs.append(opStr).append(
							propertyFileHandler.getComplexAxiomSeparator());
				}
				operandIDs.deleteCharAt(operandIDs.length()-1);				
				
				// use DB-3. for each operand, insert (op, operandIDs) as (key,value)
				// splitting up the conjunct indices across the Type1 shards
				for(String op : operandIDSet) {
					JedisShardInfo opShardInfo = 
						type12ShardedJedis.getShardInfo(op);
					HostInfo shardHostInfo = new HostInfo(opShardInfo.getHost(), 
							opShardInfo.getPort());
					pipelineConjunctIndexManager.psadd(
							shardHostInfo, op, operandIDs.toString(), 
							AxiomDB.CONJUNCT_INDEX_DB, false);
					
					pipelineManager.psadd(shardHostInfo, localKeys, 
							operandIDs.toString(), axiomDB, false);
					pipelineManager.psadd(shardHostInfo, operandIDs.toString(), 
							conceptToID(superClassExpression.toString()), axiomDB, false);
				}
				operandIDSet.clear();
			}
			else if (subClassExpression instanceof OWLObjectSomeValuesFrom) {
				// This axiom is of type3. 3r.A < B
				OWLObjectSomeValuesFrom someValuesExpression = 
								(OWLObjectSomeValuesFrom)subClassExpression;
				String role = conceptToID(someValuesExpression.getProperty().toString());
				String filler = conceptToID(someValuesExpression.getFiller().toString());
				String superClass = conceptToID(superClassExpression.toString());
				StringBuilder superClassRole = new StringBuilder(superClass).
												append(propertyFileHandler.getComplexAxiomSeparator()).
												append(role);
				
				JedisShardInfo shardInfo = type31ShardedJedis.getShardInfo(filler);
				HostInfo shardHostInfo = new HostInfo(shardInfo.getHost(), 
						shardInfo.getPort());
				// key: A, value: Br
				pipelineManager.psadd(shardHostInfo, localKeys, filler, 
						axiomDB, false);				
				pipelineManager.psadd(shardHostInfo, filler, 
						superClassRole.toString(), axiomDB, false);
			}
			else
				throw new Exception("Unexpected SubClass type of axiom. Axiom: "
						+ subClassExpression.toString());
		}
		type1ShardedJedis.disconnect();
		type12ShardedJedis.disconnect();
		type2ShardedJedis.disconnect();
		type31ShardedJedis.disconnect();
		loadRoleAxioms(normalizedOntology.getAxioms(AxiomType.SUB_OBJECT_PROPERTY),
				normalizedOntology.getAxioms(AxiomType.SUB_PROPERTY_CHAIN_OF));
	}
	
	private void loadRoleAxioms(Set<OWLSubObjectPropertyOfAxiom> subObjectPropertyAxioms,
			Set<OWLSubPropertyChainOfAxiom> subPropChainAxioms) throws Exception {
		// Axioms of type4 are handled here - r < s and r o s < t
		System.out.println("No of sub-property axiom: " + 
				subObjectPropertyAxioms.size() + "   Property chains: " + 
				subPropChainAxioms.size());
		if(subObjectPropertyAxioms.isEmpty() && subPropChainAxioms.isEmpty())
			return;
	
		List<HostInfo> type4HostInfoList = 
			typeHostMap.get(AxiomDistributionType.CR_TYPE4);
		List<JedisShardInfo> type4Shards = new ArrayList<JedisShardInfo>();
		for(HostInfo hinfo : type4HostInfoList) {
			type4Shards.add(new JedisShardInfo(hinfo.getHost(), 
					hinfo.getPort(), Constants.INFINITE_TIMEOUT));
		}
		ShardedJedis type4ShardedJedis = new ShardedJedis(type4Shards, 
											Hashing.MURMUR_HASH);
		List<HostInfo> type5HostInfoList = 
			typeHostMap.get(AxiomDistributionType.CR_TYPE5);
		List<JedisShardInfo> type5Shards = new ArrayList<JedisShardInfo>();
		for(HostInfo hinfo : type5HostInfoList) {
			type5Shards.add(new JedisShardInfo(hinfo.getHost(), 
					hinfo.getPort(), Constants.INFINITE_TIMEOUT));
		}
		ShardedJedis type5ShardedJedis = new ShardedJedis(type5Shards, 
											Hashing.MURMUR_HASH);
		// for type4, for each r < s, do (r,s) in db-0
		for(OWLSubObjectPropertyOfAxiom ax : subObjectPropertyAxioms) {
			String key = conceptToID(ax.getSubProperty().toString());			
			// copy role axioms on all role-group nodes (role axioms are less in number)
			String superPropKey = conceptToID(ax.getSuperProperty().toString());
			JedisShardInfo shardInfo = type4ShardedJedis.getShardInfo(key);
			pipelineManager.psadd(
					new HostInfo(shardInfo.getHost(), shardInfo.getPort()), 
					key, superPropKey, axiomDB, false);
		}
		System.out.println();
		// for type5, for each r o s < t, do (r,st) in db-0 and (s,rt) in db-1
		for(OWLSubPropertyChainOfAxiom ax : subPropChainAxioms) {
			List<OWLObjectPropertyExpression> propertyChain = ax.getPropertyChain();
			List<String> chainIDList = new ArrayList<String>();
			for(OWLObjectPropertyExpression pe : propertyChain) {
				String id = conceptToID(pe.toString());
				chainIDList.add(id);
			}
			if(chainIDList.size() != 2)
				throw new Exception("Role Chain greater than 2: " + propertyChain.size());
			String superPropKey = conceptToID(ax.getSuperProperty().toString());
			StringBuilder value1 = new StringBuilder(chainIDList.get(1)).
				append(propertyFileHandler.getComplexAxiomSeparator()).
				append(superPropKey);
			StringBuilder value2 = new StringBuilder(chainIDList.get(0)).
				append(propertyFileHandler.getComplexAxiomSeparator()).
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
		type4ShardedJedis.disconnect();
		type5ShardedJedis.disconnect();
	}
	
	private void mapConceptToID(OWLOntology ontology) throws Exception {
		Set<OWLClass> ontologyClasses = ontology.getClassesInSignature(); 
		Set<OWLObjectProperty> ontologyProperties = ontology.getObjectPropertiesInSignature();
		String topConcept = ontology.getOWLOntologyManager().
							getOWLDataFactory().getOWLThing().
							toString();
		String bottomConcept = ontology.getOWLOntologyManager().
								getOWLDataFactory().
								getOWLNothing().toString();
		long conceptRoleCount = 1;
		String topConceptID = Long.toString(conceptRoleCount);
		String bottomConceptID = Long.toString(Constants.BOTTOM_ID);
		
		System.out.println("No of concepts: " + ontologyClasses.size());
		System.out.println("No of roles: " + ontologyProperties.size());
		System.out.println("Total entries: " + 
				(2*ontologyClasses.size() + 2*ontologyProperties.size() + 2));
		
		HostInfo idNodeInfo = typeHostMap.get(AxiomDistributionType.CONCEPT_ID).get(0);
		HostInfo resultNodeInfo = typeHostMap.get(AxiomDistributionType.RESULT_TYPE).get(0);
		
		// add top & bottom concepts
		pipelineManager.pset(idNodeInfo, topConcept, topConceptID, axiomDB);
		pipelineManager.pset(idNodeInfo, topConceptID, topConcept, axiomDB);
		pipelineManager.pzadd(resultNodeInfo, topConceptID, 
				Constants.INIT_SCORE, topConceptID, axiomDB);
		
		pipelineManager.pset(idNodeInfo, bottomConcept, bottomConceptID, axiomDB);
		pipelineManager.pset(idNodeInfo, bottomConceptID, bottomConcept, axiomDB);
		pipelineManager.pzadd(resultNodeInfo, bottomConceptID, 
				Constants.INIT_SCORE, bottomConceptID, axiomDB);
		
		int foundBottom = 0;
		for(OWLClass cl : ontologyClasses) {
			String concept = cl.toString();
			if(concept.equals(Constants.TOP))
				continue;
			else if(concept.equals(bottomConcept)) {
				foundBottom = 1;
				continue;
			}

			conceptRoleCount++;
			String conceptID = Long.toString(conceptRoleCount);
			pipelineManager.pset(idNodeInfo, concept, conceptID, axiomDB);
			pipelineManager.pset(idNodeInfo, conceptID, concept, axiomDB);
			
			// initialize S(X) = {X,T} in the result node
			// due to storage pattern, it is X = {X} and T = {X}
			pipelineManager.pzadd(resultNodeInfo, conceptID, 
					Constants.INIT_SCORE, conceptID, axiomDB);
			pipelineManager.pzadd(resultNodeInfo, topConceptID, 
					Constants.INIT_SCORE, conceptID, axiomDB);
		}
		
		// insert foundBottom in all type3_2 nodes 
		List<HostInfo> type32Hosts = 
			typeHostMap.get(AxiomDistributionType.CR_TYPE3_2);
		for(HostInfo hostInfo : type32Hosts)
			pipelineManager.pset(hostInfo, "foundBottom", 
					Integer.toString(foundBottom), axiomDB);

		// add roles
		for(OWLObjectProperty property : ontologyProperties) {
			String role = property.toString();
			conceptRoleCount++;
			String roleID = Long.toString(conceptRoleCount);
			pipelineManager.pset(idNodeInfo, role, roleID, axiomDB);
			pipelineManager.pset(idNodeInfo, roleID, role, axiomDB);
		}
	}
	
	private String conceptToID(String concept) throws Exception {
		String conceptID = idReader.get(concept);
		if(conceptID == null)
			throw new Exception("Cannot find the concept in DB: " + concept);
		return conceptID;
	}
	
	private String constructRoleFillerFromCE(OWLObjectSomeValuesFrom classExpression) 
				throws Exception {
		String property = conceptToID(classExpression.getProperty().toString());
		String filler = conceptToID(classExpression.getFiller().toString());
		StringBuilder roleFiller = new StringBuilder(filler).
									append(propertyFileHandler.getComplexAxiomSeparator()).
									append(property);
		return roleFiller.toString();
	}
	
	public static void main(String[] args) throws Exception {
		AxiomLoader axiomLoader = new AxiomLoader();
		axiomLoader.loadAxioms(args[0], args[1]);
	}
}


