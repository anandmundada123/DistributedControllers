import java.util.ArrayList;
import java.util.SortedMap;
import java.util.TreeMap;

import org.jgroups.Address;

import com.google.common.hash.HashFunction;

public class ConsistentHash {
	private final HashFunction hashFunction;
	private final int numberOfReplicas;
	private final SortedMap<Long, Address> circle = new TreeMap<Long, Address>();

	public ConsistentHash(HashFunction hashFunction, int numberOfReplicas) {
		this.hashFunction = hashFunction;
		this.numberOfReplicas = numberOfReplicas;

	}

	public void add(ArrayList <Address> node) {
		if(node == null || node.size() == 0) {
			return;
		}
		for(Address n : node) {
			add(n);
		}
	}

	public void add(Address node) {
		for (int i = 0; i < numberOfReplicas; i++) {
			//System.out.println("Node: "+ node.toString() +"HashCode: " + hashFunction.hashBytes((node.toString() + i).getBytes()).asLong());
			circle.put(hashFunction.hashBytes((node.toString() + i).getBytes()).asLong(),
					node);
		}

	}

	public void remove(ArrayList <Address> node) {
		if(node == null || node.size() == 0) {
			return;
		}

		for(Address n : node) {
			remove(n);
		}
	}

	public void remove(Address node) {
		for (int i = 0; i < numberOfReplicas; i++) {
			circle.remove(hashFunction.hashBytes((node.toString() + i).getBytes()).asLong());
		}

	}

	public Address get(String key) {
		if (circle.isEmpty()) {
			return null;
		}
		Long hash = hashFunction.hashBytes(key.getBytes()).asLong();
		if (!circle.containsKey(hash)) {
			SortedMap<Long, Address> tailMap = circle.tailMap(hash);
			hash = tailMap.isEmpty() ? circle.firstKey() : tailMap.firstKey();
		}
		return circle.get(hash);
	}
}