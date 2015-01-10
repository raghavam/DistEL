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
	// trying 0 for infinite timeout
	public final int INFINITE_TIMEOUT = 0;	//60*60*60*1000
	public final int NUM_RULE_TYPES = 4;	
	public final int JEDIS_DEFAULT_PORT = 6379;	
	public final String TOP = "owl:Thing";	
	public final int PROXY_SERVER_PORT = 8888;	
	public final double INIT_SCORE = 1.0;	
	public final double SCORE_INCREMENT = 1.0;	
	public final String KEYS_UPDATED = "keysUpdated";	
	public final String CONJUNCT_KEYS = "conjunctKeys";
	public final String CURRENT_KEYS = "currKeys";
	public final String CURRENT_INCREMENT = "currInc";
	public final String FIELD1 = "field1";
	public final String FIELD2 = "field2";
	public final int SCRIPT_KEY_LIMIT = 60000;
	public final int KV_HOLD_LIMIT = 20000;
	public final long BOTTOM_ID = 0;
	public final long TOP_ID = 1;
	public final long LITERAL_ID = -1;
	
	public final String DOMAIN_FIELD = "domain";
	public final String RANGE_FIELD = "range";
	public final String DATA_ASSERTION_KEYS = "assertkeys";
	public final int DOMAIN_RANGE_CACHE_CAPACITY = 200;
	public final String FOUND_BOTTOM = "foundBottom";
	public final String SCORE1 = "score1";
	public final String SCORE2 = "score2";
	public final String NUM_JOBS = "numJobs";
	
	public final String PROGRESS_SORTED_SET = "pset";
	public final String PROGRESS_CHANNEL = "progress";
	public final String WAIT_FOR_ALL_MSGS = "wait";
	public final String CHUNK_KEYS = "chunkKeys";
	public final String CHUNK_KEYS2 = "chunkKeys2";
	public final String CHUNK_COUNT = "chunkCount";
	public final String TOTAL_CHUNKS = "totalChunks";
	public final String STEALERS_COUNT = "stealersCount";
	public final String STEALERS_STATUS = "stealersStatus";
	public final String STEALERS_WAIT = "stealersWait";
	public final String STEALER_BOOLEAN = "stealer";
	
	public final String SEPARATOR_COLON = ":";
	public final String SEPARATOR_RATE = "@";
	
	public final double NANO = 1000000000;
	
	public final long RANGE_BEGIN = 0;
	public final long RANGE_END = -1;
}
