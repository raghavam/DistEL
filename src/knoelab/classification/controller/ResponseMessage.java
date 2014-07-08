package knoelab.classification.controller;

/**
 * This is an enumeration of different
 * types of messages that would be exchanged 
 * between individual Classifiers and TerminationController
 *  
 * @author Raghava
 *
 */
public enum ResponseMessage {
	// messages from classifiers to TC
	UPDATE("0"), 
	NO_UPDATE("1"),
	
	// messages from TC to classifiers
	CONTINUE("2"),
	TERMINATE("3");
	
	private String code;
	
	private ResponseMessage(String code) {
		this.code = code;
	}
	
	public String getMessageCode() {
		return code;
	}	
	
	public static ResponseMessage convertCodeToMessage(String code)
	throws Exception {
	
		switch(Integer.valueOf(code)) {
			case 0: return ResponseMessage.UPDATE;
			case 1: return ResponseMessage.NO_UPDATE;
			case 2: return ResponseMessage.CONTINUE;
			case 3: return ResponseMessage.TERMINATE;
			default: throw new Exception("Unrecognised message code");
		}
	}
}
