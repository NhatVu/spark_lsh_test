package spark;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.DoubleAdder;

import org.apache.spark.mllib.linalg.SparseVector;

import com.google.gson.Gson;

public class Hasher implements Serializable {
	private static final long serialVersionUID = 1L;
	private List<Double> doubleList;

	public Hasher(List<Double> doubleList) {
		this.doubleList = doubleList;
	}

	/**
	 * Generate hashcode (0 or 1) from specific docVector
	 * 
	 * @param docVector
	 * @return 0 or 1
	 */
	public int hash(SparseVector docVector) {
		List<Double> rVector = new ArrayList<>();
		HashMap<Double, Double> valueMap = new HashMap<>();
		int size = docVector.values().length >= doubleList.size() ? doubleList.size() : docVector.values().length;
		for (int i = 0; i < size; i++) {
			rVector.add(doubleList.get(i));
			valueMap.put(doubleList.get(i), docVector.values()[i]);
		}
		DoubleAdder hashValue = new DoubleAdder();
		valueMap.entrySet().parallelStream().forEach(e -> {
			hashValue.add(e.getKey() * e.getValue());
		});
		return hashValue.doubleValue() > 0 ? 1 : 0;
	}

	public static Hasher apply(int size, long seed) {
		return new Hasher(randomMatrix(size, seed));
	}

	private static List<Double> randomMatrix(int size, long seed) {
		List<Double> buf = new ArrayList<Double>();
		Random rnd = new Random(seed);
		for (int i = 0; i < size; i++) {
			buf.add(rnd.nextGaussian() < 0 ? -1.0 : 1.0);
		}
		return buf;
	}

	@Override
	public String toString() {
		return new Gson().toJson(this);
	}

	public static void main(String[] args) {
		Hasher hasher = Hasher.apply(10, System.nanoTime());
		System.out.println(hasher.toString());
	}
}
