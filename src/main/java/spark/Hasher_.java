//package spark;
//
////import com.google.gson.Gson;
//import org.apache.spark.mllib.linalg.SparseVector;
//
//import java.io.Serializable;
//import java.util.*;
//import java.util.concurrent.atomic.DoubleAdder;
//
//public class Hasher_ implements Serializable {
//	private List<Double> doubleList;
//
//	public Hasher_(List<Double> doubleList) {
//		this.doubleList = doubleList;
//	}
//
//	public int hash(SparseVector docVector) {
//		List<Double> rVector = new ArrayList<>();
//		for(int i : docVector.indices()) {
//			System.out.print(i + ",");
//		}
//		HashMap<Double, Double> valueMap = new HashMap<>();
//		Arrays.stream(docVector.indices()).forEach(i -> {
//			System.out.println("docVectorindex: " + i );
//			System.out.println( " docVector: " + docVector.values()[i]);
//			System.out.println( "doubleList: " + doubleList.get(i));
//			rVector.add(doubleList.get(i));
//			valueMap.put(doubleList.get(i), docVector.values()[i]);
//		});
//		DoubleAdder hashValue = new DoubleAdder();
//		valueMap.entrySet().parallelStream().forEach(e -> {
//			hashValue.add(e.getKey() * e.getValue());
//		});
//		return hashValue.doubleValue() > 0 ? 1 : 0;
//	}
//
//	public static Hasher apply(int size, long seed) {
//		return new Hasher(randomMatrix(size, seed));
//	}
//
//	public static List<Double> randomMatrix(int size, long seed) {
//		List<Double> buf = new ArrayList<Double>();
//		Random rnd = new Random(seed);
//		for (int i = 0; i < size; i++) {
//			buf.add(rnd.nextGaussian() < 0 ? -1.0 : 1.0);
//		}
//		return buf;
//	}
//
//	// s
//
//	public static void main(String[] args) {
//		Hasher hasher = Hasher.apply(10, System.nanoTime());
//		System.out.println(hasher.toString());
//	}
//}
