package knoelab.classification.pipeline;

public enum PipelineMessageType {
	SET,
	GET,
	SADD, 
	SUNIONSTORE, 
	ZADD, 
	ZRANGE_BY_SCORE,
	ZRANGE,
	SMEMBERS
}
