import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

public class Client {
    
    public static void main(String[] args) {
        
        try {
            if (args.length != 4) {
                System.out.println("Please provide : ./client.sh <ip> <port> <filename> <read/write>");
                System.exit(0);
            }
            String ip = args[0];
            int port = Integer.parseInt(args[1]);
            String filename = args[2];
            String action = args[3];
            TTransport transport = new TSocket(ip, port);
            transport.open();
            
            TProtocol protocol = new TBinaryProtocol(transport);
            FileStore.Client client = new FileStore.Client(protocol);
            if(action.equals("write")){
                writeFileInClient(client, ip, port, filename);
            }else if(action.equals("read")){
                readFileInClient(client, filename);
            }
            transport.close();
        } catch (Exception ex) {
            System.err.println("Exception occured : " + ex.getMessage());
        }
    }
    
    private static void writeFileInClient(FileStore.Client client, String ip, int port, String filename) {
//        System.out.println("\n -------------------------- Inside writeFileInClient -------------------------- ");
//        String filename = "Test.txt";
        RFile rFile = new RFile();
        RFileMetadata rFileMetadata = new RFileMetadata();
        
        rFileMetadata.setFilename(filename);
        rFileMetadata.setFilenameIsSet(true);
        
        String filenameHash = convertToSHA256(filename.trim());
//        System.out.println("File name :: " + filenameHash);
//        System.out.println("ContentHash of file name :: " + filenameHash);
        try {
            Path path = Paths.get(filename);
//            System.out.println("Path :: " + path);
            byte[] byteContent = Files.readAllBytes(path);
            String content = new String(byteContent);
//            System.out.println("Content from file :: " + content);

            String contentHash = convertToSHA256(content.trim());
//            System.out.println("ContentHash of file content :: " + contentHash);
            
            rFileMetadata.setContentHash(contentHash);
            rFileMetadata.setContentHashIsSet(true);
            
//            System.out.println("RFileMetadata object from Client :: " + rFileMetadata.toString());
            
            rFile.setMeta(rFileMetadata);
            
            rFile.setContent(content);
            rFile.setContentIsSet(true);
//            System.out.println("RFile object from Client :: " + rFile.toString());

            NodeID destinationNode = client.findSucc(filenameHash);
//            System.out.println("Destination Node from writeFileInClient :: " + destinationNode.toString());
           
            TTransport transport = new TSocket(destinationNode.getIp(), destinationNode.getPort());
            transport.open();
            TProtocol protocol = new TBinaryProtocol(transport);
            client = new FileStore.Client(protocol);
            client.writeFile(rFile);
            
        } catch (IOException ex) {
            System.err.println("Exception occured while accessing file. File does not exist. " + ex.getMessage());
            System.exit(0);
        } catch (SystemException ex) {
            System.err.println("SystemException occured while writing file : " + ex.getMessage());
            System.exit(0);
        } catch (TException ex) {
            System.err.println("TException occured while executing writeFileTesting() : " + ex.getMessage());
            System.exit(0);
        }
//        System.out.println(" -------------------------- Done writeFileInClient-------------------------- \n\n");
        
    }
    
    private static void readFileInClient(FileStore.Client client, String filename) {
//        System.out.println("\n -------------------------- Inside readFileInClient --------------------------  ");
        //String filename = "Test.txt";
        String filenameHash = convertToSHA256(filename);
//        System.out.println("Filename : " + filename);
//        System.out.println("FilenameHash : " + filenameHash);
        try {
            NodeID destinationNode = client.findSucc(filenameHash);
//            System.out.println("destinationNode from readFileInClient : " + destinationNode.toString());
            TTransport transport = new TSocket(destinationNode.getIp(), destinationNode.getPort());
            transport.open();
            TProtocol protocol = new TBinaryProtocol(transport);
            client = new FileStore.Client(protocol);
            RFile rFile = client.readFile(filename);
            
            if (rFile != null) {
                String content = rFile.getContent();
                int version = rFile.getMeta().getVersion();
                String rFileFilename = rFile.getMeta().getFilename();
                String contentHash = rFile.getMeta().getContentHash();
                
                System.out.println("Successfully read the file.");
                System.out.println("Filename : " + rFileFilename);
                System.out.println("Version : " + version);
                System.out.println("Content Hash : " + contentHash);
                System.out.println("Content : " + content);
                
            }
        } catch (SystemException e) {
            System.err.println("SystemException occured while reading file : " + e.getMessage());
            System.exit(0);
        } catch (TException e) {
            System.err.println("Exception occured in readFileInClient() : " + e.getMessage());
            System.exit(0);
        }
//        System.out.println(" -------------------------- Done readFileInClient ------------------------- \n\n");
        
    }
    
    public static String convertToSHA256(String key) {
        StringBuilder stringBuilder = new StringBuilder();
        try {
            if (key != null && !key.isEmpty()) {
                MessageDigest messageDigest = MessageDigest.getInstance("SHA-256");
                messageDigest.update(key.trim().getBytes());
                byte[] byteArray = messageDigest.digest();
                for (int i = 0; i < byteArray.length; i++) {
                    stringBuilder.append(String.format("%02x", byteArray[i]));
                }
            }
//            System.out.println("SHA-256 Converstion for the client ::  " + key + " : " + stringBuilder);
        } catch (NoSuchAlgorithmException ex) {
            System.err.println("Exception occured while converting to SHA-256 : " + ex.getMessage());
            System.exit(0);
        }
        return stringBuilder.toString();
    }
}
