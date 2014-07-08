package knoelab.classification.misc;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import knoelab.classification.init.AxiomDistributionType;


/**
 * This class handles all the read requests for
 * the ShardInfo.properties file. 
 * 
 * @author Raghava
 */
public class PropertyFileHandler {
	private final static PropertyFileHandler propertyFileHandler = 
			new PropertyFileHandler();
	private Properties shardInfoProperties = null;
	private final String PROPERTY_FILE = "ShardInfo.properties";
	
	private PropertyFileHandler() {
		// does not allow instantiation of this class
		try {
			shardInfoProperties = new Properties();
			shardInfoProperties.load(new FileInputStream(PROPERTY_FILE));
		}
		catch(FileNotFoundException e) {
			e.printStackTrace();
		}
		catch(IOException e) {
			e.printStackTrace();
		}
	}
	
	public static PropertyFileHandler getInstance() {
		return propertyFileHandler;
	}
	
	public Object clone() throws CloneNotSupportedException {
		throw new CloneNotSupportedException("Cannot clone an instance of this class");
	}
	
	public HostInfo getLocalHostInfo() {
		String[] hostPort = shardInfoProperties.getProperty("shard.local").split(":");
		String localMachineName = null;
		try {
			//it is better use machine name rather than 'localhost'
			localMachineName = InetAddress.getLocalHost().getHostName();
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}
		HostInfo localhostInfo = new HostInfo(localMachineName, 
				Integer.parseInt(hostPort[1]));
		return localhostInfo;
	}
/*	
	public List<HostInfo> getAllShardsInfo() {
		List<HostInfo> hostList = new ArrayList<HostInfo>();
		String shardCountStr = shardInfoProperties.getProperty("shard.count");
		int shardCount = Integer.parseInt(shardCountStr);
		for(int i=1; i<=shardCount; i++) {			
			String[] hostPort = shardInfoProperties.getProperty("shard" + i).trim().split(":");
			HostInfo hostInfo = new HostInfo(hostPort[0], Integer.parseInt(hostPort[1]));
			hostList.add(hostInfo);
		}		
		return hostList;
	}
*/	
	public String getAxiomSymbol() {
		return shardInfoProperties.getProperty("axiom.symbol");
	}
	
	public String getAxiomPropertyChainForwardSymbol() {
		return shardInfoProperties.getProperty("axiom.propertychain.forward.symbol");
	}
	
	public String getAxiomPropertyChainReverseSymbol() {
		return shardInfoProperties.getProperty("axiom.propertychain.reverse.symbol");
	}
	
	public String getResultRoleConceptSymbol() {
		return shardInfoProperties.getProperty("result.role.concept.symbol");
	}
	
	public String getResultRoleCompoundKey1Symbol() {
		return shardInfoProperties.getProperty("result.role.compound.key1.symbol");
	}
	
	public String getResultRoleCompoundKey2Symbol() {
		return shardInfoProperties.getProperty("result.role.compound.key2.symbol");
	}
	
	public String getQueueSymbol() {
		return shardInfoProperties.getProperty("queue.symbol");
	}
	
	public String getSuperclassSymbol() {
		return shardInfoProperties.getProperty("result.superclass.symbol");
	}
	
//	public String getComplexAxiomSeparator() {
//		return shardInfoProperties.getProperty("complex.axiom.separator");
//	}
	
	public String getLocalKeys() {
		return shardInfoProperties.getProperty("kvstore.localkeys");
	}
	
	public String getDatePattern() {
		return shardInfoProperties.getProperty("datepattern");
	}
	
	public String getClassifierChannel() {
		return shardInfoProperties.getProperty("classifier.channel");
	}
	
	public String getTerminationControllerChannel() {
		return shardInfoProperties.getProperty("terminationcontroller.channel");
	}
	
	public String getTimestampKey() {
		return shardInfoProperties.getProperty("timestamp");
	}
	
	public HostInfo getTerminationControllerLocation() {
		String[] hostPort = shardInfoProperties.getProperty("tc.location").split(":");
		HostInfo tcHostInfo = new HostInfo(hostPort[0], Integer.parseInt(hostPort[1]));
		return tcHostInfo;
	}
/*	
	public int getShardCount() {
		return Integer.parseInt(shardInfoProperties.getProperty("shard.count"));
	}
*/	
	public String getEquivalentClassKeys() {
		return shardInfoProperties.getProperty("equiclass.keys");
	}
	
	public String getAxiomEquivalentClassSymbol() {
		return shardInfoProperties.getProperty("axiom.equivalentclass.symbol");
	}
	
	public String getCharset() {
		return shardInfoProperties.getProperty("charset");
	}
	
	public int getPipelineQueueSize() {
		String maxSize = shardInfoProperties.getProperty("pipeline.queue.size");
		return Integer.parseInt(maxSize);
	}
	
	public Map<AxiomDistributionType, Double> getAllTypesWeightMap() {
		Map<AxiomDistributionType, Double> typesWeightMap = 
			new HashMap<AxiomDistributionType, Double>();
		AxiomDistributionType[] axiomTypes = AxiomDistributionType.values();
		for(AxiomDistributionType axiomType : axiomTypes) {
			if(axiomType == AxiomDistributionType.CONCEPT_ID || 
					axiomType == AxiomDistributionType.RESULT_TYPE)
				continue;
			String weightStr = shardInfoProperties.getProperty(
									axiomType.toString());
			double weight;
			String[] numDenom = weightStr.split("/");
			if(numDenom.length == 2) 
				weight = Double.parseDouble(numDenom[0])/
							Double.parseDouble(numDenom[1]);
			else
				weight = Double.parseDouble(weightStr);
			typesWeightMap.put(axiomType, weight);
		}
		return typesWeightMap;
	}
	
	public List<HostInfo> getAllNodes() {
		List<HostInfo> allNodes = new ArrayList<HostInfo>();
		String csvHosts = shardInfoProperties.getProperty("NODES_LIST");
		String[] hosts = csvHosts.split(",");
		for(String hostPort : hosts) {
			String[] hpSplit = hostPort.split(":");
			allNodes.add(new HostInfo(hpSplit[0], 
					Integer.parseInt(hpSplit[1])));
		}
		return allNodes;
	}
	
	public int getNodeCount() {
		String csvHosts = shardInfoProperties.getProperty("NODES_LIST");
		String[] hosts = csvHosts.split(",");
		return hosts.length;
	}
	
	public HostInfo getConceptIDNode() {
		String hostInfo = shardInfoProperties.getProperty(
						AxiomDistributionType.CONCEPT_ID.toString());
		String[] hpSplit = hostInfo.split(":");
		return new HostInfo(hpSplit[0], Integer.parseInt(hpSplit[1]));
	}
	
	public HostInfo getResultNode() {
		String hostInfo = shardInfoProperties.getProperty(
						AxiomDistributionType.RESULT_TYPE.toString());
		String[] hpSplit = hostInfo.split(":");
		return new HostInfo(hpSplit[0], Integer.parseInt(hpSplit[1]));
	}
	
	public boolean isWorkStealingEnabled() {
		String isEnabledStr = shardInfoProperties.getProperty(
				"work.stealing.enabled");
		if(isEnabledStr.equals("true"))
			return true;
		else
			return false;
	}
	
	public boolean isInstrumentationEnabled() {
		String isEnabledStr = shardInfoProperties.getProperty(
				"instrumentation.enabled");
		if(isEnabledStr.equals("true"))
			return true;
		else
			return false; 
	}
	
	public int getType32SizeFactor() {
		return Integer.parseInt(
				shardInfoProperties.getProperty("TYPE32_SIZE_FACTOR"));
	}
	
	public HostInfo getNormalizerCache() {
		String hostInfo = shardInfoProperties.getProperty("NORMALIZE_CACHE");
		String[] hpSplit = hostInfo.split(":");
		return new HostInfo(hpSplit[0], Integer.parseInt(hpSplit[1]));
	}
	
	public String getClassPrefix() {
		return shardInfoProperties.getProperty("class.prefix");
	}
	
	public String getPropertyPrefix() {
		return shardInfoProperties.getProperty("property.prefix");
	}
	
	public String getAxiomTypeKey() {
		return shardInfoProperties.getProperty("axiom.type");
	}
	
	public String getChannelKey() {
		return shardInfoProperties.getProperty("channel");
	}
	
	public String getAllChannelsKey() {
		return shardInfoProperties.getProperty("channels.key");
	}
	
	public String getSubRoleLHSKey() {
		return shardInfoProperties.getProperty("subRoleLHS.key");
	}
	
	public String getSubRoleRHSKey() {
		return shardInfoProperties.getProperty("subRoleRHS.key");
	}
	
	public String getRoleChainLHSKey() {
		return shardInfoProperties.getProperty("chainLHS.key");
	}
	
	public String getRoleChainRHSKey() {
		return shardInfoProperties.getProperty("chainRHS.key");
	}
	
	public HostInfo getChannelHost() {
		String host = shardInfoProperties.getProperty("channel.host");
		String[] hostPort = host.split(":");
		return new HostInfo(hostPort[0], Integer.parseInt(hostPort[1]));
	}
	
	public int getChunkSize() {
		String chunkSize = shardInfoProperties.getProperty("chunk.size");
		return Integer.parseInt(chunkSize);
	}
}


