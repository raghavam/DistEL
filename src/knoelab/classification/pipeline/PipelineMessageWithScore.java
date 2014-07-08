package knoelab.classification.pipeline;

public class PipelineMessageWithScore extends PipelineMessage {

	private double score;
	private double minScore;
	private double maxScore;
	
	public PipelineMessageWithScore(String key, String value, double score, 
			PipelineMessageType messageType, boolean collectPipelineResponse) {
		super(key, value, messageType, collectPipelineResponse);
		this.score = score;
		// either 'score' field is used or minScore & maxScore are used
		minScore = maxScore = 0;
	}
	
	public PipelineMessageWithScore(String key, double minScore, 
			double maxScore, PipelineMessageType messageType, 
			boolean collectPipelineResponse) {
		super(key, null, messageType, collectPipelineResponse);
		this.minScore = minScore;
		this.maxScore = maxScore;
		// either 'score' field is used or minScore & maxScore are used
		score = -1.0;
	}

	public void setScore(double score) {
		this.score = score;
	}

	public double getScore() {
		return score;
	}

	public void setMinScore(double minScore) {
		this.minScore = minScore;
	}

	public double getMinScore() {
		return minScore;
	}

	public void setMaxScore(double maxScore) {
		this.maxScore = maxScore;
	}

	public double getMaxScore() {
		return maxScore;
	}
}
