import java.net.InetAddress;
import java.net.UnknownHostException;

import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;
import org.apache.thrift.transport.TTransportException;

public class Server {

    public static FileStoreHandler fileHandler;
    public static FileStore.Processor<FileStore.Iface> fileProcessor;
    public static int port;
    public static String ip;

    public static void main(String[] args) {
    try {
       if (args.length != 1) {
        System.out.println("Please provide :  ./server.sh <port> ");
        System.exit(0);
       }
	
	    port = Integer.parseInt(args[0]);
	    ip = InetAddress.getLocalHost().getHostAddress();
//        System.out.println("IP :: " + ip + " port :: " + port);

        fileHandler = new FileStoreHandler(ip, port);
        fileProcessor = new FileStore.Processor<FileStore.Iface>(fileHandler);

        Runnable simple = new Runnable() {
            public void run() {
                startServer(fileProcessor);
            }
            };
        new Thread(simple).start();
        } catch (Exception ex) {
            System.err.println("Exception occured :" + ex.getMessage());
            System.exit(0);
        }
        
    }

    public static void startServer(FileStore.Processor<FileStore.Iface> processor) {
	try {
	    TServerTransport serverTransport = new TServerSocket(port);

	    TServer server = new TThreadPoolServer(new TThreadPoolServer.Args(serverTransport).processor(processor));
        System.out.println("Server started at : ");
	    System.out.println("Server NodeID: " + fileHandler.currentNodeID.getId() + "\nServer IP: " + fileHandler.ip
			       + "\nServer Port: " + fileHandler.port + "\n");
	    server.serve();
        } catch (TTransportException e) {
            System.err.println("Error in opening TTransport socket: " + e.getMessage());
            System.exit(0);
        }
    }
}
