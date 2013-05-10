package knoelab.classification.misc;


/**
 * An interface which holds all the global (project level) constants
 * @author Raghava
 *
 */
public interface Constants {

	// specifies the number of bytes to be allocated
	public final int NUM_BYTES = 4;
	// used while creating a Jedis instance
	public final int INFINITE_TIMEOUT = 60*60*1000;	
	public final int NUM_RULE_TYPES = 4;	
	public final int JEDIS_DEFAULT_PORT = 6379;	
	public final String TOP = "owl:Thing";	
	public final int PROXY_SERVER_PORT = 8888;	
	public final double INIT_SCORE = 1.0;	
	public final double SCORE_INCREMENT = 1.0;	
	public final String KEYS_UPDATED = "keysUpdated";	
	public final String CONJUNCT_KEYS = "conjunctKeys";
	public final String CURRENT_KEYS = "currKeys";
	public final String FIELD1 = "field1";
	public final String FIELD2 = "field2";
	public final int SCRIPT_KEY_LIMIT = 60000;
	public final int KV_HOLD_LIMIT = 20000;
	public final long BOTTOM_ID = 0;
}
