package knoelab.classification.pipeline;

public class PipelineMessageWithField extends PipelineMessage {
	//used for hset method
	private String field;
	
	public PipelineMessageWithField(String key, String field, String value, 
			PipelineMessageType messageType, boolean collectPipelineResponse) {
		super(key, value, messageType, collectPipelineResponse);
		this.setField(field);
	}

	public void setField(String field) {
		this.field = field;
	}

	public String getField() {
		return field;
	}
}
