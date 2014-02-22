import java.util.ArrayList;
import java.util.SortedMap;
import java.util.TreeMap;

import com.google.common.hash.HashFunction;

public class ConsistentHash<T> {
	private final HashFunction hashFunction;
	private final int numberOfReplicas;
	private final SortedMap<Integer, T> circle = new TreeMap<Integer, T>();

	public ConsistentHash(HashFunction hashFunction, int numberOfReplicas) {
		this.hashFunction = hashFunction;
		this.numberOfReplicas = numberOfReplicas;

	}

	public void add(ArrayList <T> node) {
		if(node == null || node.size() == 0) {
			return;
		}
		for(T n : node) {
			add(n);
		}
	}

	public void add(T node) {
		for (int i = 0; i < numberOfReplicas; i++) {
			circle.put(hashFunction.hashBytes((node.toString() + i).getBytes()).asInt(),
					node);
		}

	}

	public void remove(ArrayList <T> node) {
		if(node == null || node.size() == 0) {
			return;
		}

		for(T n : node) {
			remove(n);
		}
	}

	public void remove(T node) {
		for (int i = 0; i < numberOfReplicas; i++) {
			circle.remove(hashFunction.hashBytes((node.toString() + i).getBytes()).asInt());
		}

	}

	public T get(Object key) {
		if (circle.isEmpty()) {
			return null;
		}
		Integer hash = hashFunction.hashBytes(key.toString().getBytes()).asInt();
		if (!circle.containsKey(hash)) {
			SortedMap<Integer, T> tailMap = circle.tailMap(hash);
			hash = tailMap.isEmpty() ? circle.firstKey() : tailMap.firstKey();
		}
		return circle.get(hash);
	}
}