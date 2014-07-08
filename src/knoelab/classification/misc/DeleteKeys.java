package knoelab.classification.misc;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisShardInfo;
import redis.clients.jedis.Pipeline;

/**
 * This class is used to delete all the keys from all the shards or localhost.
 * 
 * @author Raghava
 *
 */
public class DeleteKeys {

	private List<JedisShardInfo> shards;
	
	public DeleteKeys(String mode) {
		shards = new ArrayList<JedisShardInfo>();
		
		if(mode.equals("local")) {
			shards.add(new JedisShardInfo("localhost", 6379));
		}
		else if(mode.equals("distributed")) {
			PropertyFileHandler propertyFileHandler = PropertyFileHandler.getInstance();
			List<HostInfo> hostInfoList = propertyFileHandler.getAllNodes();
			hostInfoList.add(propertyFileHandler.getConceptIDNode());
			hostInfoList.add(propertyFileHandler.getResultNode());
			for(HostInfo hostInfo : hostInfoList)
				shards.add(new JedisShardInfo(hostInfo.getHost(), hostInfo.getPort()));	
		}
		else {
			System.out.println("Specify a mode (local/distributed)");
			System.exit(-1);
		}
	}
	
	public void deleteAll() {
		boolean anotherAttempt = true; 
		for(JedisShardInfo shard : shards) {
			Jedis jedis = new Jedis(shard.getHost(), shard.getPort());
			while(anotherAttempt) {
				try {
					jedis.flushAll();
					anotherAttempt = false;
				}
				catch(Exception e) {
					anotherAttempt = true;
					System.out.println(e.getMessage());
					System.out.println(shard.getHost() + ":" + shard.getPort() + 
							" Exception - trying again");
				}
			}
			jedis.disconnect();
			anotherAttempt = true;
		}
		System.out.println("All Keys in " + shards.size() + " shards are deleted");
	}
	
	public void delete(String regex) throws IOException {
		for(JedisShardInfo shard : shards) {
			Jedis jedis = new Jedis(shard.getHost(), shard.getPort());
			final Set<String> keys = jedis.keys(regex);
			Pipeline p = jedis.pipelined();
			for(String key : keys)
				p.del(key);
			p.sync();
			jedis.disconnect();
		}
		System.out.println("Keys with " + regex + " pattern in " + shards.size() + " shards are deleted");
	}
	
	// supposed to delete all the entries which were computed after loading axioms.
	// But is not possible currently because S(X) gets initialized while loading axioms.
	public void deleteComputedEntries() {
		throw new UnsupportedOperationException("not implemented");
	}
	
	public static void main(String[] args) throws IOException {
		DeleteKeys deleteKeys = new DeleteKeys(args[0]);
		deleteKeys.deleteAll();
	}
}
