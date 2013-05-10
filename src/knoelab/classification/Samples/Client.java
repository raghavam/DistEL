package knoelab.classification.Samples;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;

public class Client {

	public static void main(String[] args) throws IOException {
		Socket socket = null;
		BufferedOutputStream writer = null;
		InetAddress localHost = null;
		
		try {
			socket = new Socket("nimbus.cs.wright.edu", 8888);
			writer = new BufferedOutputStream(socket.getOutputStream());
			localHost = InetAddress.getLocalHost();
		} 
		catch (UnknownHostException e) {
			System.err.println("Don't know about host: nimbus.cs.wright.edu");
            System.exit(1);
			e.printStackTrace();
		} 
		catch (IOException e) {
			System.err.println("Couldn't get I/O for the connection to: nimbus.cs.wright.edu");
            System.exit(1);
			e.printStackTrace();
		}
		StringBuilder msg1 = new StringBuilder(localHost.getHostName() + " - ");
		for(int i=1; i<=3; i++)
			msg1.append(i);
		byte[] b = msg1.toString().getBytes("UTF-8");
		writer.write(b);
		System.out.println(b.length);
		
		System.out.println("Building second msg");
		StringBuilder msg2 = new StringBuilder(localHost.getHostName() + " - ");
		for(int i=4; i<=6; i++)
			msg2.append(i);
		b = msg2.toString().getBytes("UTF-8");
		writer.write(b);
		System.out.println(b.length);
		
		System.out.println("Building third msg");
		StringBuilder msg3 = new StringBuilder(localHost.getHostName() + " - ");
		for(int i=7; i<=9; i++)
			msg3.append(i);
		b = msg3.toString().getBytes("UTF-8");
		writer.write(b);
		System.out.println(b.length);
		
		writer.close();
		socket.close();
	}
}
