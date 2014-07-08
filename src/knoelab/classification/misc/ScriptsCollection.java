package knoelab.classification.misc;

public interface ScriptsCollection {
	
	public final String scriptCurrentKeysAdd = 
		"local size = redis.call('ZCARD', KEYS[1]) " +
		"local score " +
		"local highestEScore " +
		"if(size == 0) then " +
		   	"score = 1.0 " +
		"else " +
		   	"highestEScore = redis.call('ZRANGE', KEYS[1], " +
		   			"-1, -1, 'WITHSCORES') " +
		   	"score = highestEScore[2] + " + 
		   			Constants.SCORE_INCREMENT + " " +
		"end " +
		"for index,value in pairs(ARGV) do " +
			"redis.call('ZADD', KEYS[1], score, value) " +
		"end "; 
	
	public final String insertClassAssertions = 
		"local size = redis.call('ZCARD', 'currKeys') " +
		"local score " +
		"local highestEScore " +
		"if(size == 0) then " +
		   	"score = 1.0 " +
		"else " +
		   	"highestEScore = redis.call('ZRANGE', 'currKeys', " +
		   			"-1, -1, 'WITHSCORES') " +
		   	"score = highestEScore[2] + " + 
		   			Constants.SCORE_INCREMENT + " " +
		"end " +
		"local ret " + 
		"local unique = 0 " +
		"for index,value in pairs(ARGV) do " +
			"ret = redis.call('SADD', KEYS[index], value) " +
			"unique = unique + ret " +
			"if(ret > 0) then " +
				"redis.call('ZADD', 'currKeys', score, KEYS[index]) " +
				"redis.call('SADD', 'localkeys', KEYS[index]) " +
			"end " +
		"end " +
		"return unique ";
	
	public final String insertClassAssertions1 = 
		"local toBeAddedList " +
		"local escore " +
		"local score " +
		"local ret " +
		"local unique = 0 " + 
		"for index1,value1 in pairs(ARGV) do " +
			"toBeAddedList = redis.call('ZRANGE', KEYS[index1], 0, -1) " +
			"escore = redis.call('ZRANGE', value1, -1, -1, 'WITHSCORES') " +
		  	"score = escore[2] + " + Constants.SCORE_INCREMENT + " " +
		  	"for index2,value2 in pairs(toBeAddedList) do " +
		  		"if(not redis.call('ZSCORE', value1, value2)) then " +
		  			"ret = redis.call('ZADD', value1, score, value2) " +
		  			"unique = unique + ret " +
		  		"end " +
		  	"end " +
		"end " +
		"return unique ";
	
	public final String insertClassAssertions2 = 
		"local toBeAddedList " +
		"local escore " +
		"for index1,value1 in pairs(ARGV) do " +
			"toBeAddedList = redis.call('ZRANGE', KEYS[index1], 0, -1) " +
			"escore = redis.call('ZRANGE', value1, -1, -1, 'WITHSCORES') " +
			"return escore[2] " +
		"end ";
	
	public final String insertInBottom = 
		"local size = redis.call('ZCARD', KEYS[1]) " +
		"local score " +
		"local highestEScore " +
		"if(size == 0) then " +
		   	"score = 1.0 " +
		"else " +
		   	"highestEScore = redis.call('ZRANGE', KEYS[1], " +
		   			"-1, -1, 'WITHSCORES') " +
		   	"score = highestEScore[2] + " + 
		   			Constants.SCORE_INCREMENT + " " +
		"end " +
		"local ret " + 
		"local unique = 0 " +
		"for index,value in pairs(ARGV) do " +
			"ret = redis.call('ZADD', KEYS[1], score, value) " +
			"unique = unique + ret " +
		"end " +
		"return unique "; 
	
	public final String processedChunkStatusScript = 
		"local chunkCount = redis.call('DECR', '" + Constants.STEALERS_COUNT + "') " +
		"if(chunkCount == 0) then " +
			"redis.call('LPUSH', '" + Constants.STEALERS_WAIT + "', 'dummy') " +
		"end " +
		"redis.call('SADD', '" + Constants.STEALERS_STATUS + "', ARGV[1]) " +
		"return chunkCount ";
	
	public final String decrAndGetChunk = 
			"local rangeIndex = tonumber(ARGV[1]) - 1 " + 
			"local chunk = redis.call('ZRANGE', KEYS[1], 0, rangeIndex) " +
			"local size = #chunk " +
			"local chunkCount = -1 " +
			"if(size > 0) then " +
				"redis.call('ZREMRANGEBYRANK', KEYS[1], 0, rangeIndex) " +
				"chunkCount = redis.call('INCR', '" + Constants.CHUNK_COUNT + "') " +
			"end " +
			"local t = {} " +
			"t[1] = tostring(chunkCount) " +
			"for index,value in pairs(chunk) do " +
				"table.insert(t, value) " +
			"end " +
			"return t ";
	
	public final String decrAndGetStealerChunk = 
			"local rangeIndex = tonumber(ARGV[1]) - 1 " + 
			"local chunk = redis.call('ZRANGE', KEYS[1], 0, rangeIndex) " +
			"local size = #chunk " +
			"local chunkCount = -1 " +
			"if(size > 0) then " +
				"redis.call('ZREMRANGEBYRANK', KEYS[1], 0, rangeIndex) " +
				"chunkCount = redis.call('INCR', '" + Constants.CHUNK_COUNT + "') " +
				"redis.call('SET', '" + Constants.STEALER_BOOLEAN + "', 'false') " +
				"redis.call('INCR', '" + Constants.STEALERS_COUNT + "') " +		
			"end " +
			"local totalChunks = redis.call('GET', '" + Constants.TOTAL_CHUNKS + "') " +
			"local t = {} " +
			"t[1] = tostring(chunkCount) " +
			"t[2] = tostring(totalChunks) " +
			"for index,value in pairs(chunk) do " +
				"table.insert(t, value) " +
			"end " +
			"return t ";
}
