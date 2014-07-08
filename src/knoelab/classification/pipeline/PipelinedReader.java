package knoelab.classification.pipeline;

import knoelab.classification.misc.AxiomDB;
import knoelab.classification.misc.HostInfo;

public interface PipelinedReader {

	public void psmembers(HostInfo hostInfo, String key, AxiomDB axiomDB);
	
	public void pZRangeByScore(HostInfo hostInfo, String key, double minScore, 
			double maxScore, AxiomDB axiomDB);
	
	public void pZRangeByScoreWithScores(HostInfo hostInfo, String key, double minScore, 
			double maxScore, AxiomDB axiomDB);
	
	public void pZRange(HostInfo hostInfo, String key, 
			long startIndex, long endIndex, AxiomDB axiomDB);
	
	public void pget(HostInfo hostInfo, String key, AxiomDB axiomDB);
	
	public boolean pZScore(HostInfo hostInfo, String key, String member, 
			AxiomDB axiomDB);
}
