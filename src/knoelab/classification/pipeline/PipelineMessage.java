package knoelab.classification.pipeline;

public class PipelineMessage {
	private String key;
	private String value;
	private PipelineMessageType messageType;
	private boolean collectPipelineResponse;
	
	public PipelineMessage(String key, String value, 
			PipelineMessageType messageType, boolean collectPipelineResponse) {
		this.key = key;
		this.value = value;
		this.messageType = messageType;
		this.collectPipelineResponse = collectPipelineResponse;
	}

	public void setKey(String key) {
		this.key = key;
	}

	public String getKey() {
		return key;
	}

	public void setValue(String value) {
		this.value = value;
	}

	public String getValue() {
		return value;
	}

	public void setMessageType(PipelineMessageType messageType) {
		this.messageType = messageType;
	}

	public PipelineMessageType getMessageType() {
		return messageType;
	}

	public void setCollectPipelineResponse(boolean collectPipelineResponse) {
		this.collectPipelineResponse = collectPipelineResponse;
	}

	public boolean isCollectPipelineResponse() {
		return collectPipelineResponse;
	}		
}
