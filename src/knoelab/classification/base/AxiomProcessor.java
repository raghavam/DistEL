package knoelab.classification.base;

import java.util.List;

import knoelab.classification.misc.HostInfo;
import knoelab.classification.pipeline.PipelineManager;
import redis.clients.jedis.Jedis;

/**
 * Common interface for all TypeAxiomProcessors
 * @author Raghava
 *
 */
public interface AxiomProcessor {

	public boolean processOneWorkChunk(List<String> chunkKeys, 
			PipelineManager pipelinedReader, HostInfo keysHostInfo, 
			Double currentIncrement, Jedis keyStore) throws Exception;
	public void sendProgressMessage(double progress, int iterationCount);
	public void cleanUp();
	
}
