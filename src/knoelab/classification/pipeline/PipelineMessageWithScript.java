package knoelab.classification.pipeline;

import java.util.List;

/**
 * The objects of this class hold messages containing information for 
 * pipelining eval commands.
 * 
 * @author Raghava
 */
public class PipelineMessageWithScript extends PipelineMessage {
	
	private String script;
	private List<String> keys;
	private List<String> args;

	public PipelineMessageWithScript(String script, List<String> keys, 
			List<String> args) {
		super(null, null, PipelineMessageType.EVAL, true);
		this.script = script;
		this.keys = keys;
		this.args = args;
	}

	public String getScript() {
		return script;
	}
	
	public List<String> getScriptKeys() {
		return keys;
	}
	
	public List<String> getScriptArgs() {
		return args;
	}
}
