package knoelab.classification.samples;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import knoelab.classification.misc.Constants;

public class Server {

	public static void main(String[] args) throws IOException {
		ServerSocket serverSocket = null;
        boolean listening = true;
        ExecutorService threadExecutor = Executors.newCachedThreadPool();
 
        try {
            serverSocket = new ServerSocket(Constants.PROXY_SERVER_PORT);
        } catch (IOException e) {
            System.err.println("Could not listen on port: " + Constants.PROXY_SERVER_PORT);
            System.exit(-1);
        }
 
        while (listening) {
        	Socket clientSocket = serverSocket.accept();
        	threadExecutor.execute(new ProcessData(clientSocket));
        }
        threadExecutor.shutdown();
        serverSocket.close();
	}
}

class ProcessData implements Runnable {

	private Socket clientSocket;
	
	ProcessData(Socket socket) {
		clientSocket = socket;
	}
	
	@Override
	public void run() {
		try {
			BufferedInputStream reader = new BufferedInputStream(clientSocket.getInputStream());
			byte[] clientData = new byte[13];
			int bytesRead;
        	do {
        		bytesRead = reader.read(clientData, 0, clientData.length);
        		if(bytesRead != -1)
        			System.out.println(new String(clientData, "UTF-8"));
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
