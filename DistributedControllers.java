
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.List;
import org.jgroups.Address;
import org.jgroups.JChannel;
import org.jgroups.Message;
import org.jgroups.ReceiverAdapter;
import org.jgroups.View;

public class DistributedControllers extends ReceiverAdapter{

	private JChannel channel;
	private String user_name;
	//private ArrayList <String> ipPool;
	// Store own address
	private Address ownAddr;
	// Store Address of master 
	private Address masterAddr;
	// boolean variable to check if I am master or not
	private boolean isMaster;
	
	public DistributedControllers() {
		user_name =System.getProperty("user.name", "n/a");
		//ipPool = new ArrayList<String>();
		isMaster = false;
		masterAddr  = null;
	}
	private void start() throws Exception {
        channel=new JChannel(); // use the default config, udp.xml
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
	            line="[" + user_name + "] " + line;
	            Message msg=new Message(null, null, line);
	            channel.send(msg);
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
	private void sendMesssage(Address dest, Address src, Object msg) {
		Message m=new Message(dest, src, msg);
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
	public void viewAccepted(View new_view) {
		//System.out.println(new_view.getCreator());
		if (ownAddr == null) {
			ownAddr = channel.getAddress();
			System.out.println("Own Address:" + ownAddr);
		}
		List <Address> addrList = new_view.getMembers();
		int clustSize = addrList.size();
		if(clustSize == 1) { 
			// I am the only one in cluster so I am master
			masterAddr = ownAddr;
			isMaster = true;
			System.out.println("I am master");
			return;
		} else if(masterAddr != null && !addrList.contains(masterAddr)){
			// Master is crashed 
			masterAddr = addrList.get(0);
			if (masterAddr.equals(ownAddr)) {
				isMaster = true;
				System.out.println("I am master");
				sendMesssage(null, ownAddr, ControllerConstants.MASTER_ID_UPDATE);
				return;
			} 
		}

		if(isMaster) {
			sendMesssage(null, ownAddr, ControllerConstants.MASTER_ID_UPDATE);
		}
	}

	public void receive(Message msg) {
	    final String m =  msg.toStringAsObject();
	    if(m.equals(ControllerConstants.MASTER_ID_UPDATE)) {
	    	masterAddr = msg.getSrc();
	    	if(masterAddr.equals(ownAddr)) {
	    		isMaster = true;
	    	} else {
	    		System.out.println("master Addr :" +  masterAddr);
	    		System.out.println("own Addr :" +  ownAddr);
	    		System.out.println("\n\t Got Master :" + masterAddr);
	    	}
	    }
	}

	
	public static void main(String[] args) throws Exception{
		// TODO Auto-generated method stub
		new DistributedControllers().start();
	}

}
