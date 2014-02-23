
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;

import org.jgroups.Address;
import org.jgroups.JChannel;
import org.jgroups.Message;
import org.jgroups.ReceiverAdapter;
import org.jgroups.View;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;


public class DistributedControllers extends ReceiverAdapter{

	// Channel to communicate with each other
	private JChannel channel;
	// Store own address
	private Address ownAddr;
	// Store Address of master 
	private Address masterAddr;
	// boolean variable to check if I am master or not
	private boolean isMaster;
	// ethernet counter, this is useful while creating Alias
	private int ethCnt;
	// Hash function which uses consistent hashing
	private ConsistentHash <Address> hashFun;
	// Global IP Address
	private ArrayList <String> globalIpPool;
	// Local IP List
	private ArrayList<String> localIpPool;
	// Local to ethernet mapping
	private HashMap<String, String> localIpEthMap;
	// List of all members in cluster
	private ArrayList <Address> addrList;
	
	/**
	 * Constrcutor, Initialize all data structures
	 * @throws Exception 
	 */
	public DistributedControllers() {
		// Read List of Gloabal IP Addrss from File 
		globalIpPool = ControllerUtils.readGlobalIpPool(ControllerConstants.IP_POOL_FILE_LOC);
		localIpPool = new ArrayList<String>();
		localIpEthMap = new HashMap<String, String>();
		ethCnt = 0;
		isMaster = false;
		masterAddr  = null;
		HashFunction hf = Hashing.sha256();
		hashFun = new ConsistentHash<Address> (hf, 2);	
		addrList = new ArrayList<Address>();
	}
	
	private void start() throws Exception {
         // use the default config, udp.xml
		channel=new JChannel();
        // Store own address
        channel.setReceiver(this);
        channel.connect(ControllerConstants.CHANNEL_NAME);
        eventLoop();
        channel.close();
    }
	
	private void eventLoop() {
	    BufferedReader in=new BufferedReader(new InputStreamReader(System.in));
	    while(true) {
	        try {
	            System.out.print("> "); System.out.flush();
	            String line=in.readLine().toLowerCase();
	            if(line.startsWith("quit") || line.startsWith("exit"))
	                break;
	            if(line.startsWith("local")) {
	            	System.out.println(localIpPool);
	            } else if (line.startsWith("global")) {
	            	System.out.println(globalIpPool);
	            } else if (line.startsWith("address")) {
	            	System.out.println("Master Address:" + masterAddr);
	            	System.out.println("my Address:" + ownAddr);
	            } else if (line.startsWith("clear")) {
	            	ControllerUtils.executeCmdGetStatus("clear");
	            } else if (line.startsWith("interfaces")) {
	            	System.out.println(localIpEthMap);
	            } else if (line.startsWith("nodes")) {
	            	System.out.println(addrList);
	            } else if (line.startsWith("help")) {
	            	System.out.println("You can enter following commands:");
	            	System.out.println("local\t\tPrint all ip address assigned to this node");
	            	System.out.println("global\t\tPrint all ip address in IP Pool");
	            	System.out.println("address\t\tPrint master address and own address");
	            	System.out.println("clear\t\tclear Screen");
	            	System.out.println("nodes\t\tList of all nodes in cluster");
	            	System.out.println("Interfaces\t\t Print all interfaces");
	            }
	        }
	        catch(Exception e) {      	
	        }
	    }
	}
	
	/**
	 * Send message
	 * @param dest
	 * @param src
	 * @param msg
	 * @throws Exception
	 */
	private void sendMesssage(Address dest, Object msg) {
		Message m=new Message(dest, ownAddr, msg);
		try {
			channel.send(m);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	/**
	 * This method will be get executed when a new member has joined the 
	 * group or that an existing member has left or crashed.
	 */
	//@SuppressWarnings("unchecked")
	public void viewAccepted(View new_view) {
		// Get own address
		if (ownAddr == null) {
			ownAddr = channel.getAddress();
			System.out.println("Own Address:" + ownAddr);
		}
		
		ArrayList<Address> oldAddrList = new ArrayList<Address>(addrList);
		ArrayList<Address> removeAddrList = new ArrayList<Address>(addrList);
		//System.out.println("\n\t View Creator : " + new_view.getCreator());
		addrList = new  ArrayList <Address>(new_view.getMembers());
		ArrayList<Address> addAddrList = new ArrayList<Address>(addrList);
		
		addAddrList.removeAll(oldAddrList);
		removeAddrList.removeAll(addrList);
		hashFun.add(addAddrList);
		hashFun.remove(removeAddrList);
		System.out.println("Add Address List: " + addAddrList);
		System.out.println("Remove Address List: " + removeAddrList);
		System.out.println("orig add List: " + addrList);

		int clustSize = addrList.size();
		if(clustSize == 1) { 
			// I am the only one in cluster so I am master
			masterAddr = ownAddr;
			isMaster = true;
			System.out.println("I am master, only I am left");
		} else if(masterAddr != null && !addrList.contains(masterAddr)) {
			// Master is crashed 
			masterAddr = addrList.get(0);
			if (masterAddr.equals(ownAddr)) {
				isMaster = true;
				System.out.println("I am master - multiple cluser in nodes");
			} 
		}

		if(isMaster) {
			
			// If new node got added, then send message to it, to let him know who is master
			if(addAddrList.size() != 0 ) {
				for(Address a: addAddrList ) {
					sendMesssage(a, ControllerConstants.MASTER_ID_UPDATE_MSG);
				}
			}
			//Find out which server will serve every IP Address
			int gIpSize = globalIpPool.size();
			for(int i = 0; i < gIpSize; i++) {
				Address newAddr = hashFun.get(globalIpPool.get(i));
				String msg = ControllerConstants.ADDR_ADD_MSG + ControllerConstants.MSG_INFO_SEPERATOR + newAddr.toString() +ControllerConstants.MSG_INFO_SEPERATOR + globalIpPool.get(i);
				sendMesssage(null, msg);
			}
			
		}
	}

	public void receive(Message msg) {
		if (ownAddr == null) {
			ownAddr = channel.getAddress();
			System.out.println("Own Address:" + ownAddr);
		}
	    final String m =  msg.toStringAsObject();
	    if(m.equals(ControllerConstants.MASTER_ID_UPDATE_MSG)) {
	    	masterAddr = msg.getSrc();
	    	if(masterAddr.equals(ownAddr)) {
	    		isMaster = true;
	    	} else {
	    		System.out.println("master Addr :" +  masterAddr);
	    		System.out.println("own Addr :" +  ownAddr);
	    		System.out.println("\n\t Got Master :" + masterAddr);
	    	}
	    } else if (m.startsWith(ControllerConstants.ADDR_ADD_MSG)) {
	    	String msgPart[] = m.split(ControllerConstants.MSG_INFO_SEPERATOR);
	    	String newAdd = msgPart[1];
	    	String ipAdd = msgPart[2];
	    	if(newAdd.equals(ownAddr.toString())) {
	    		if(!localIpPool.contains(ipAdd)) {
	    			System.out.println("Add IP Address to list: " + ipAdd);
	    			String interfaceName = ControllerConstants.ETHER_NAME + ethCnt;
	    			ethCnt++;
	    			try {
						ControllerUtils.setAlias(interfaceName, ipAdd);
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
	    			localIpEthMap.put(ipAdd, interfaceName);
	    			localIpPool.add(ipAdd);
	    			if(addrList.size() > 1) {
	    				String remMsg = ControllerConstants.ADDR_REM_MSG + ControllerConstants.MSG_INFO_SEPERATOR + ipAdd;
	    				System.out.println("\n\t Sending Remove message");
	    				sendMesssage(null, remMsg);
	    			}
	    		}
	    	} 
	    }else if (m.startsWith(ControllerConstants.ADDR_REM_MSG)) {
	    	String msgPart[] = m.split(ControllerConstants.MSG_INFO_SEPERATOR);
	    	String ipAdd = msgPart[1];
	    	if(!ownAddr.equals(msg.getSrc()) && localIpPool.contains(ipAdd)) {
	    		System.out.println("Removing IP Address from my list: " + ipAdd);
	    		String interfaceName = localIpEthMap.get(ipAdd);
	    		try {
					ControllerUtils.removeAlias(interfaceName);
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
	    		localIpEthMap.remove(ipAdd);
	    		localIpPool.remove(ipAdd);
	    	}
	    }
	}

	
	public void clearAllAliases() throws IOException, InterruptedException {
		for(String str : localIpEthMap.values()) {
			ControllerUtils.removeAlias(str);
		}
	}
	
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		DistributedControllers dc= new DistributedControllers();
		try {
			dc.start();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			System.out.println("In Finally, Clearing all " );
			try {
				dc.clearAllAliases();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
}
