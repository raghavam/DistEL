package knoelab.classification.pipeline;

import knoelab.classification.misc.AxiomDB;
import knoelab.classification.misc.HostInfo;

public interface PipelinedWriter {

	public void pset(HostInfo hostInfo, String key, String value,
			AxiomDB axiomDB);

	public void psadd(HostInfo hostInfo, String key, String value,
			AxiomDB axiomDB, boolean collectPipelineResponse);

	public void psunionstore(HostInfo hostInfo, String key,
			String value, AxiomDB axiomDB, boolean collectPipelineResponse);

	public void pzadd(HostInfo hostInfo, String key, double score, 
			String value, AxiomDB axiomDB);
}