package knoelab.classification.misc;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;

import redis.clients.jedis.Jedis;

/**
 * This class takes snapshots of the DB at specified intervals.
 * These snapshots can be used to measure the completeness% later
 * on. 
 * 
 * @author Raghava
 *
 */
public class ResultSnapshotter {

	private Jedis localStore;
	// 10 mins in millis
	private final int INIT_WAIT_TIME = 5 * 60 * 1000;
	private final int INTERVAL = 2 * 60 * 1000;
	private final int TOTAL_TIME = 15 * 60 * 1000;
	
	public void takeSnapshot(String rdbPath) throws Exception {
		PropertyFileHandler propertyFileHandler = 
			PropertyFileHandler.getInstance();
		HostInfo localHostInfo = propertyFileHandler.getLocalHostInfo();
		localStore = new Jedis(localHostInfo.getHost(), localHostInfo.getPort());
		File rdbfile = new File(rdbPath);
		String fileName = rdbfile.getName().split("\\.")[0];
		
		int runningTimeTotal = INIT_WAIT_TIME;
		// wait till the initial wait time.
		System.out.println("Waiting....");
		Thread.sleep(runningTimeTotal);
		// do a background save and copy the DB file
		localStore.bgsave();
		copyFile(rdbfile, runningTimeTotal, fileName);
		System.out.println("Copied file at " + runningTimeTotal/1000);
		runningTimeTotal += INTERVAL;
		
		while(runningTimeTotal <= TOTAL_TIME) {
			System.out.println("\nWaiting....");
			Thread.sleep(INTERVAL);
			localStore.bgsave();
			copyFile(rdbfile, runningTimeTotal, fileName);
			System.out.println("Copied file at " + runningTimeTotal/1000);
			runningTimeTotal += INTERVAL;
		}		
		localStore.disconnect();
	}
	
	private void copyFile(File rdbfile, int currentTime, 
			String fileName) throws IOException {
		File dupfile = new File(fileName + "-" + currentTime/1000 + ".rdb");
		if(!dupfile.exists())
			dupfile.createNewFile();
		FileChannel source = null;
		FileChannel destination = null;
		try {
			source = new FileInputStream(rdbfile).getChannel();
        	destination = new FileOutputStream(dupfile).getChannel();
        	long count = 0;
        	long size = source.size();              
        	while((count += destination.transferFrom(source, count, size-count))<size);
		}
		finally {
			if(source != null) {
	            source.close();
	        }
	        if(destination != null) {
	            destination.close();
	        }
		}
	}
	
	public static void main(String[] args) throws Exception {
		if(args.length != 1) {
			System.out.println("Provide the path to the rdb file");
			System.exit(-1);
		}
		new ResultSnapshotter().takeSnapshot(args[0]);
	}

}
