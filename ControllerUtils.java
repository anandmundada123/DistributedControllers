import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Class to write all utility functions 
 * @author anandmundada
 *
 */

public class ControllerUtils {
	
	static final String IPADDRESS_PATTERN = 
			"^([01]?\\d\\d?|2[0-4]\\d|25[0-5])\\." +
			"([01]?\\d\\d?|2[0-4]\\d|25[0-5])\\." +
			"([01]?\\d\\d?|2[0-4]\\d|25[0-5])\\." +
			"([01]?\\d\\d?|2[0-4]\\d|25[0-5])$";
	static  Pattern pattern = Pattern.compile(IPADDRESS_PATTERN);;
	static  Matcher matcher;
	
	static boolean validateIP(final String ip){		  
		  matcher = pattern.matcher(ip);
		  return matcher.matches();	    	    
	}
	
	/**
	 * 
	 * @return HashMap<IpAdd, Interface Name>
	 * @throws SocketException
	 */
	static HashMap<String, String> getAllIpAdd() throws SocketException {
		
		HashMap <String, String> ipAdds = new HashMap<String, String> ();
		Enumeration<NetworkInterface> e=NetworkInterface.getNetworkInterfaces();
        while(e.hasMoreElements())
        {
            NetworkInterface n=(NetworkInterface) e.nextElement();
            Enumeration<InetAddress> ee = n.getInetAddresses();
            while(ee.hasMoreElements())
            {
                InetAddress i= (InetAddress) ee.nextElement();
                if(validateIP(i.getHostAddress()))
                	ipAdds.put(i.getHostAddress(), n.getName());
            }
        }
		return ipAdds;
	}
	
	/**
	 * 
	 * @param cmd
	 * @return True if command was successfully executed
	 * 		   False if there is some problem while running a command  
	 * @throws IOException
	 * @throws InterruptedException
	 */
	static boolean executeCmdGetStatus(String cmd) throws IOException, InterruptedException {
		Runtime rt = Runtime.getRuntime();
		Process proc = rt.exec(cmd);
		proc.waitFor();
		int exitVal = proc.exitValue();
	    if(exitVal == 0) {
	    	return true;
	    } else {
	    	BufferedReader reader = 
	   	         new BufferedReader(new InputStreamReader(proc.getInputStream()));
	   	    String output = "";
	   	    String line = "";			
	   	    while ((line = reader.readLine())!= null) {
	   	    	output = output.concat(line).concat("\n");
	   	    }
	   	    System.out.println(output);
	    	return false;
	    }
	}
	
	static String executeCmdGetOp(String cmd) throws IOException, InterruptedException {
		Runtime rt = Runtime.getRuntime();
		Process proc = rt.exec(cmd);
		proc.waitFor();
	    BufferedReader reader = 
	         new BufferedReader(new InputStreamReader(proc.getInputStream()));
	    String output = "";
	    String line = "";			
	    while ((line = reader.readLine())!= null) {
	    	output = output.concat(line).concat("\n");
	    }
	    return output;
	}
	
	static String getIpAddress() {
		try {
			return (InetAddress.getLocalHost().getHostAddress());
		} catch (UnknownHostException e) {
			e.printStackTrace();
			return null;
		}
	}
	
	static String getHostName() {
		try {
			return (InetAddress.getLocalHost().getHostName());
		} catch (UnknownHostException e) {
			e.printStackTrace();
			return null;
		}
	}
	
	
	static boolean removeAlias(String interfaceName) throws IOException, InterruptedException  {
		String cmd = "sudo ifconfig " + interfaceName +" down";
		return executeCmdGetStatus(cmd);
		
	}
	
	static boolean setAlias(String interfaceName, String ip) throws IOException, InterruptedException  {
		String cmd = "sudo ifconfig " + interfaceName +" " + ip + " up";
		return executeCmdGetStatus(cmd);
		
	}
	
	static boolean broadCastArpPkt(String ipAdd) throws IOException, InterruptedException {
		String cmd = "sudo arping -S " + ipAdd + " -c 1 -B";
		return executeCmdGetStatus(cmd);
	}
	
	/**
	 * Read file and print out all information
	 * This is useful to read all global IP Address
	 * @param file
	 */
	static ArrayList<String> readGlobalIpPool(String file) {
		BufferedReader br = null;
		ArrayList <String> ipPool = new ArrayList<String>();
		try {
			String sCurrentLine;
			br = new BufferedReader(new FileReader(file));
			while ((sCurrentLine = br.readLine()) != null) {
				ipPool.add(sCurrentLine.trim());
			}
		} catch (IOException e) {
			e.printStackTrace();	
		} finally {
			try {
				if (br != null)
					br.close();
			} catch (IOException ex) {
				ex.printStackTrace();
			}
		}
		return ipPool;
	}
	
}
