package knoelab.classification.test;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.Set;

import knoelab.classification.init.AxiomDistributionType;
import knoelab.classification.misc.Constants;
import knoelab.classification.misc.PropertyFileHandler;
import knoelab.classification.misc.Util;

import org.semanticweb.owlapi.apibinding.OWLManager;
import org.semanticweb.owlapi.model.IRI;
import org.semanticweb.owlapi.model.OWLObjectProperty;
import org.semanticweb.owlapi.model.OWLOntology;
import org.semanticweb.owlapi.model.OWLOntologyManager;

import redis.clients.jedis.Jedis;

public class RoleValuesTest {

	public static void checkRoleValues(String ontPath) throws Exception {
		
		Jedis localStore = new Jedis("localhost", 6379);
		try {
			OWLOntologyManager manager = OWLManager.createOWLOntologyManager();
			File ontFile = new File(ontPath);
			IRI documentIRI = IRI.create(ontFile);
			OWLOntology ontology = manager.loadOntologyFromOntologyDocument(documentIRI);
			Set<OWLObjectProperty> objectProperties = ontology.getObjectPropertiesInSignature();
			System.out.println("No of object roles: " + objectProperties.size() + "\n");
			
			// get host location of IDs
			String[] hostPort = localStore.hget("TypeHost", 
							AxiomDistributionType.CONCEPT_ID.toString()).trim().split(":");
			String[] type3_2HostPort = localStore.hget("TypeHost", 
							AxiomDistributionType.CR_TYPE3_2.toString()).trim().split(":");
			String[] resultHostPort = localStore.hget("TypeHost", 
					AxiomDistributionType.RESULT_TYPE.toString()).trim().split(":");
			Jedis idReader = new Jedis(hostPort[0], Integer.parseInt(hostPort[1]));
			Jedis resultStore = new Jedis(resultHostPort[0], Integer.parseInt(resultHostPort[1]));
			Jedis type3_2Store = new Jedis(type3_2HostPort[0], Integer.parseInt(type3_2HostPort[1]));
			localStore.select(1);
			
			PropertyFileHandler propertyFileHandler = PropertyFileHandler.getInstance();
			
			String checkRole1 = "<http://www.co-ode.org/ontologies/galen#isFeatureOf>";
			String checkRole2 = "<http://www.co-ode.org/ontologies/galen#isCountConcentrationOf>";
			String checkConcept1 = "<http://knoelab.wright.edu/ontologies#Class884>";
			String checkConcept2 = "<http://knoelab.wright.edu/ontologies#Class1262>";
			String checkConcept3 = "<http://www.co-ode.org/ontologies/galen#RaisedNeutrophilCount>";
			
			String role1ID = Util.conceptToID(checkRole1, idReader);
			String role2ID = Util.conceptToID(checkRole2, idReader);
			String concept1ID = Util.conceptToID(checkConcept1, idReader);
			String concept2ID = Util.conceptToID(checkConcept2, idReader);
			String concept3ID = Util.conceptToID(checkConcept3, idReader);
			String localKeys = propertyFileHandler.getLocalKeys();
/*			
			ByteBuffer conceptRolePair = ByteBuffer.allocate(2*Constants.NUM_BYTES).
											put(concept1ID).put(role1ID);
			Set<byte[]> db0members = type3_2Store.smembers(conceptRolePair.array());
			System.out.println("iFO.884 exists in DB0: " + db0members.size());
			System.out.println("884.iFO in localKeys: " + 
					type3_2Store.sismember(localKeys, conceptRolePair.array()));
			System.out.println("RNC member of 1262: " + resultStore.sismember(concept2ID, concept3ID));
			type3_2Store.select(1);
			Set<byte[]> db1members = type3_2Store.smembers(conceptRolePair.array());
			System.out.println("iFO.884 exists in DB1: " + db1members.size());
			
			System.out.println("DB0 members");
			for(byte[] member : db0members)
				System.out.println("\t" + Util.idToConcept(member, idReader));
			System.out.println("\nDB1 members");
			for(byte[] member : db1members)
				System.out.println("\t" + Util.idToConcept(member, idReader));
			
			Set<byte[]> intersectVals = localStore.sinter(role1ID, role2ID);
			System.out.println("\nNo of intersection vals: " + intersectVals.size());
			
			// 
			
			
			for(byte[] val : intersectVals) {
				byte[][] xyPair = Util.extractFragments(val);
				String x = Util.idToConcept(xyPair[0], idReader);
				String y = Util.idToConcept(xyPair[1], idReader);
				System.out.println("\t(" + x + ", " + y + ")");
			}
*/			
			idReader.disconnect();
			type3_2Store.disconnect();
			resultStore.disconnect();
		}
		finally {
			localStore.disconnect();
		}
	}
		
	public static void main(String[] args) throws Exception {
		if(args.length != 1 || args[0].isEmpty()) {
			System.out.println("Give the path of owl file");
    		System.exit(-1);
		}
		checkRoleValues(args[0]);
	}

}
