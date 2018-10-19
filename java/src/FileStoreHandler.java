import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

public class FileStoreHandler implements FileStore.Iface {
    
    public int port;
    public String ip;
    public NodeID currentNodeID;
    public Map<String, RFile> metaInformation;
    
    public List<NodeID> fingerTable;
    
    public FileStoreHandler(String ip, int port) {
        this.port = port;
        this.ip = ip;
        String key = ip + ":" + port;
        currentNodeID = new NodeID(convertToSHA256(key), ip, port);
        metaInformation = new LinkedHashMap<String, RFile>();
    }
    
    /**
     *
     * @param rFile
     * @return
     *
     */
    @Override
    public void writeFile(RFile rFile) throws SystemException, TException {
//        System.out.println("\n ------------------------FileStoreHandler : Inside writeFile------------------------");
            String filename = rFile.getMeta().getFilename();
            File file = new File(filename);
            String SHA256_fileId = convertToSHA256(filename.trim());
    //        System.out.println("SHA256_fileId : " + SHA256_fileId);
            NodeID nodeID = findSucc(SHA256_fileId);
    //        System.out.println("Destination NodeId :: " + nodeID.toString());
    //        System.out.println("Current NodeID :: " + currentNodeID);
          
            /**
             * If the server does not own the file id, the server is not the file
             * successor, a SystemException should be thrown
             */
            if (!nodeID.equals(currentNodeID)) {
                System.err.println("The server does not own the file id.");
                throw new SystemException().setMessage("The server does not own the file id.");
            }
        
        try {
            String rFileContent = rFile.getContent();
            RFileMetadata rFileMetadata = rFile.getMeta();
            String fileContentHash = "";
          
            /**
             * If the filename does not exist on the server, a new file should be created
             * with its version attribute set to 0.
             */
            if (!metaInformation.containsKey(file.getName())) {
    //            System.out.println("-------- file does not exist --------");
                rFile.setContent(rFileContent);
                rFileMetadata.setVersion(0);
                
                if (rFileContent != null) {
                    fileContentHash = convertToSHA256(rFileContent.trim());
                    rFileMetadata.setContentHash(fileContentHash);
                }
                rFile.setMeta(rFileMetadata);
    //            System.out.println("file not exist --- RFile Object " + rFile.toString());
                
            } else {
    //            System.out.println("-------- file exist --------");
                
                /**
                 * If the filename exists on the server - the file contents should be
                 * overwritten, and the version number should be incremented.
                 */
                
                RFile rFile_existing = metaInformation.get(file.getName());
    //            System.out.println("rFile_existing object : " + rFile_existing.toString());
                RFileMetadata rFileMetadata_existing = rFile_existing.getMeta();
                rFileMetadata_existing.setVersion(rFileMetadata_existing.getVersion() + 1);
                //Overwrite rFileContent old content with new content
                rFileContent = rFile.getContent();
                fileContentHash = convertToSHA256(rFileContent.trim());
                rFileMetadata_existing.setContentHash(fileContentHash);
                
                //RFILE
                rFile_existing.setContent(rFileContent);
                rFile_existing.setMeta(rFileMetadata_existing);
                rFile = rFile_existing;
    //            System.out.println("file exist ---- RFile Object :: " + rFile.toString());
            }
            metaInformation.put(filename, rFile);
          
            FileWriter fileWriter = null;
            BufferedWriter bufferedWriter = null;
            fileWriter = new FileWriter(file, false);
            bufferedWriter = new BufferedWriter(fileWriter);
            bufferedWriter.write(rFile.getContent());
            bufferedWriter.close();
            
        } catch (IOException ex) {
            System.err.println("Exception occured from writeFile." + ex.getMessage());
        }
//        System.out.println("--------------------------FileStoreHandler : DONE writeFile ------------------------\n\n");
    }
    
    /**
     *
     * @param filename
     * @return rFile
     *
     */
    
    @Override
    public RFile readFile(String filename) throws SystemException, TException {
//        System.out.println("\n------------------------ FileStoreHandler : Inside readFile ------------------------");
            String filenameHash = convertToSHA256(filename);
            NodeID node = findSucc(filenameHash);            
            if (!(node.equals(currentNodeID))) {
                System.err.println("The server does not own the file id.");
                throw new SystemException().setMessage("Exception from readFile, the server does not own the file id.");
            }
            
            File file = new File(filename);
            
            if (!metaInformation.containsKey(file.getName())) {
                System.err.println("File does not exist on the server.");
                throw new SystemException().setMessage("Exception while reading File : File does not exist on the server.");
            }
            RFile existing_rFile = metaInformation.get(filename);
            
            return existing_rFile;
    }

    
    /**
     *
     * @param node_list
     * @return
     *
     */
    @Override
    public void setFingertable(List<NodeID> node_list) throws TException {
        fingerTable = node_list;
    }
    
    /**
     *
     * @param key
     * @return NodeID
     *
     */
    
    @Override
    public NodeID findSucc(String key) throws SystemException, TException {
//        System.out.println("\n ------------  FileStoreHandler : Inside findSucc ------------ ");
        
        NodeID successorNode = null;
        try {
            NodeID predecessorNode = findPred(key);
//            System.out.println("Predecessor Node :: " + predecessorNode.toString());
            String predecessorNodeIP = "";
            int predecessorNodePort = 0;
            
            if (predecessorNode != null) {
                predecessorNodeIP = predecessorNode.getIp();
                predecessorNodePort = predecessorNode.getPort();
            }
            TTransport transport = new TSocket(predecessorNodeIP, predecessorNodePort);
            transport.open();
            TProtocol protocol = new TBinaryProtocol(transport);
            FileStore.Client client = new FileStore.Client(protocol);
            if (client != null) {
                successorNode = client.getNodeSucc();
            }
        } catch (TTransportException e) {
            System.err.println("Exception occured while establishing TTransport socket connection : " + e.getMessage());
        } catch (SystemException e) {
            System.err.println("Exception occured while finding successor : " + e.getMessage());
        } catch (TException e) {
            System.err.println("Exception occured : " + e.getMessage());
        }
        
//        System.out.println("Successor Node :::   " + successorNode.toString());
       // System.out.println(" ------------ FileStoreHandler : Done findSucc ------------  \n ");
        return successorNode;
        
    }
    
    /**
     *
     * @param key
     * @return NodeID
     */
    @Override
    public NodeID findPred(String key) throws SystemException, TException {
      //  System.out.println("\n ------------------  FileStoreHandler : Inside findPred ------------------ ");
        if (fingerTable == null) {
            System.err.println("Fingertable is not set, Please execute the init script.");
            throw new SystemException().setMessage("Exception occured : Fingertable is not set, Please execute the init script.");
        }
        NodeID currentNode = currentNodeID;
        NodeID successorNode = fingerTable.get(0);
        if (!inBetween(key, currentNode.getId(), successorNode.getId())) {
            for (int i = fingerTable.size() - 1; i > 0; i--) {
                if (inBetween(fingerTable.get(i).getId(), currentNode.getId(), key)) {
                    return recursive_findPred(key, fingerTable.get(i));
                }
            }
        }
        
       // System.out.println("------------------  FileStoreHandler : Done findPred ------------------ \n ");

        return currentNode;

    }
    
    
    private boolean inBetween(String key, String currentNodeId, String successorNodeId) {
        int current_successorNode = currentNodeId.compareTo(successorNodeId);
        int key_currentNode = key.compareTo(currentNodeId);
        int key_successorNode = key.compareTo(successorNodeId);
        if (current_successorNode < 0) {
            if ((key_currentNode > 0 && key_successorNode <= 0)) {
                return true;
            } else {
                return false;
            }
        } else {
            if (((key_currentNode > 0 && key_successorNode >= 0) || (key_currentNode < 0 && key_successorNode <= 0))) {
                return true;
            } else {
                return false;
            }
        }
        
    }
    
    
    /**
     *
     * @param key
     * @param fingerTableEntry
     * @return
     */
    public NodeID recursive_findPred(String key, NodeID fingerTableEntry) {
        try {
            TTransport tTransport = new TSocket(fingerTableEntry.getIp(), fingerTableEntry.getPort());
            tTransport.open();
            TProtocol protocol = new TBinaryProtocol(tTransport);
            FileStore.Client client = new FileStore.Client(protocol);
            return client.findPred(key);
            
        } catch (TTransportException e) {
            System.err.println("Exception occured while establishing TTransport socket connection : " + e.getMessage());
        } catch (SystemException e) {
            System.err.println(e.getMessage());
        } catch (TException e) {
            System.err.println("Exception occured : " + e.getMessage());
        }
        return fingerTableEntry;
    }
    
    /**
     *
     * @param
     * @return NodeID
     */
    @Override
    public NodeID getNodeSucc() throws SystemException, TException {
        if (fingerTable == null) {
            System.err.println("Fingertable is not set, Please execute the init file.");
            throw new SystemException()
            .setMessage("Exception while getting fingertable node successor.");
        }
        return fingerTable.get(0);
    }
    
    /**
     *
     * @param key
     * @return
     */
    public String convertToSHA256(String key) {
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
        } catch (NoSuchAlgorithmException ex) {
            System.err.println("Exception occured while converting to SHA-256 : " + ex.getMessage());
        }
        return stringBuilder.toString();
    }
}


