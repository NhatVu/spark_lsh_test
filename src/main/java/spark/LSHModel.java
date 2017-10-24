package spark;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.linalg.SparseVector;
import org.apache.spark.mllib.util.Saveable;
import scala.Tuple2;
import shapeless.newtype;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class LSHModel implements Serializable, Saveable {


    /**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private Map<Long, Hasher> hashFunctions = new HashMap<Long, Hasher>();
    private JavaPairRDD<Tuple2<Integer, String>, Long> signatureMatix = null; // (group, concat hash), vectorId
    private int numBands; //num bands
    private int numRows; //num rows in a band

    public LSHModel(int m, int numRows, int numBands) {
        this.numBands = numBands;
        this.numRows = numRows;
        for (int i = 0; i < numRows * numBands; i++) {
            hashFunctions.put(Long.valueOf(i), Hasher.apply(m,1001));
        }
    }

    // calculate hashValue for new doc
    public List<Tuple2<Integer, String>> hashValue(SparseVector data) {
        //(bandId, concat hashcode)
        List<Tuple2<Integer, String>> bandID_HashCode = new ArrayList<Tuple2<Integer, String>>();
            int hashFId = 0;
            int minhash = 0;
            for (int i = 0; i < numBands; i++) {
                String concatHashKey = "";
                for (int j = 0; j < numRows; j++) {
                    hashFId = i * numRows + j;
                    minhash = getHashFunctions().get(hashFId).hash(data);
                    concatHashKey += minhash;
                    //List hash value cua hashFid
                }
                bandID_HashCode.add(new Tuple2<Integer, String>(i, concatHashKey));
            }
        
        return bandID_HashCode;
    }

    //get candidate
    public JavaRDD<Long> getCandidates(long vId) {
        // get all buckets containing vectorId
        List<Tuple2<Integer, String>> buckets = signatureMatix.filter(r -> r._2() == vId)
                .map(x -> x._1()).distinct().collect();
        // get all vector id in the same bucket with input vector id
        return signatureMatix.filter(r -> buckets.contains(r._1()))
                .map(x -> x._2())
                .filter(x -> x != vId);
    }

    public JavaRDD<Long> getCandidates(SparseVector data){
        List<Tuple2<Integer, String>> hashValue = hashValue(data);
        return signatureMatix.filter(r -> hashValue.contains(r._1()))
                .map(x -> x._2());
    }

    public LSHModel add (Long vId, SparseVector v, JavaSparkContext sc) {
        List<Tuple2<Tuple2<Integer, String>, Long>> m = hashValue(v).stream()
                .map(a -> new Tuple2<>(a, vId)).collect(Collectors.toList());
        JavaPairRDD<Tuple2<Integer, String>, Long> newData = sc.parallelize(m).mapToPair(r -> r);
        signatureMatix = signatureMatix.union(newData);
        return this;
    }

    public LSHModel remove(Long vId, JavaSparkContext sc) {
        signatureMatix = signatureMatix.filter(r -> r._2() != vId);
        return  this;
    }

    public void save(JavaSparkContext sc, String pathFile) {
        return;
    }


    public static class SaveLoad {
        private String thisFormatVersion = "0.0.1";
        private String thisClassName = this.getClass().getName();

        public static void save(JavaSparkContext sc, LSHModel model, String pathFile) {

        }
    }




    public Map<Long, Hasher> getHashFunctions() {
        return hashFunctions;
    }

    public void setHashFunctions(Map<Long, Hasher> hashFunctions) {
        this.hashFunctions = hashFunctions;
    }

    public JavaPairRDD<Tuple2<Integer, String>, Long> getSignatureMatix() {
        return signatureMatix;
    }

    public void setSignatureMatix(JavaPairRDD<Tuple2<Integer, String>, Long> signatureMatix) {
        this.signatureMatix = signatureMatix;
    }

    @Override
    public void save(SparkContext sc, String path) {

    }

    @Override
    public String formatVersion() {
        return null;
    }
}
