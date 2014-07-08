package knoelab.classification;


/**
 * Proxy server for Redis. It takes in
 * the (k,v) pairs and adds a score to it.
 * Then adds it to a zset (through a pipeline).
 * 
 * @author Raghava
 *
 */
@Deprecated
public class ProxyScorer {
	
/*	
	private double score = 1.0;
	private final double increment = 0.0001;
	private HostInfo resultNode;

	public void registerServer() throws IOException {
		PropertyFileHandler propertyFileHandler = PropertyFileHandler.getInstance();
		resultNode = propertyFileHandler.getResultNode();
		List<HostInfo> targetHosts = new ArrayList<HostInfo>();
		targetHosts.add(resultNode);
		
		ServerSocket serverSocket = null;
        boolean listening = true;
        ExecutorService threadExecutor = Executors.newSingleThreadExecutor();
        PipelineManager pipelineManager = new PipelineManager(targetHosts, 
        		propertyFileHandler.getPipelineQueueSize());
        GregorianCalendar cal1 = new GregorianCalendar();
        
        try {
            serverSocket = new ServerSocket(Constants.PROXY_SERVER_PORT);
        } 
        catch (IOException e) {
            System.err.println("Could not listen on port: " + Constants.PROXY_SERVER_PORT);
            System.exit(-1);
        }
 
        while (listening) {
        	Socket clientSocket = serverSocket.accept();
        	threadExecutor.execute(new ClientDataProcessor(clientSocket, 
        			incrementAndGet(), pipelineManager, resultNode));
        	
        	// using time based diff. to force pipeline synch periodically
        	GregorianCalendar cal2 = new GregorianCalendar();
			long diff = cal2.getTimeInMillis() - cal1.getTimeInMillis();
			if(diff >= 5 * 60 * 1000) {
				cal1 = cal2;
				pipelineManager.synchAll(AxiomDB.NON_ROLE_DB);
			}
        }
        // TODO: The above infinite loop should be terminated and 
        // 		 resources closed
        threadExecutor.shutdown();
        serverSocket.close();
        pipelineManager.synchAndCloseAll(AxiomDB.NON_ROLE_DB);
	}
	
	private double incrementAndGet() {
		score = score + increment;
		return score;
	}
	
	public static void main(String[] args) {

	}
*/	
}

/*
class ClientDataProcessor implements Runnable {

	private Socket clientSocket;
	private double score;
	private PipelinedWriter pipelineManager;
	private HostInfo resultNode;
	
	ClientDataProcessor(Socket socket, double score, 
			PipelinedWriter pmanager, HostInfo resultNode) {
		clientSocket = socket;
		this.score = score;
		this.pipelineManager = pmanager;
		this.resultNode = resultNode;
	}
	
	@Override
	public void run() {
		try {
			BufferedInputStream reader = new BufferedInputStream(clientSocket.getInputStream());
			byte[] clientData = new byte[2*Constants.NUM_BYTES];
			int bytesRead;
        	do {
        		bytesRead = reader.read(clientData, 0, clientData.length);
        		if(bytesRead != -1) {
        			// split data into key & value
        			byte[][] keyValue = Util.extractFragments(clientData);
        			pipelineManager.pzadd(resultNode, keyValue[0], score, keyValue[1]);
        		}        			
        	}
        	while(bytesRead != -1);
        	
        	reader.close();
        	clientSocket.close();
		} 
		catch (IOException e) {
			e.printStackTrace();
		}
	}
	
}
*/
