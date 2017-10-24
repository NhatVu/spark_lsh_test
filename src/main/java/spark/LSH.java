package spark;

import com.google.common.collect.Lists;
import com.google.common.primitives.Ints;

import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.mllib.linalg.SparseVector;
import scala.Tuple2;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class LSH implements Serializable {
    // private static final Logger LOGGER = LoggerFactory.getLogger(LSH.class);

    /**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private transient Logger logger = Logger.getLogger(LSH.class);
	public static double NOVELTY_THRESHOLD = 0.8;
    public static int TABLE_COUNT = 15;
    public static int HYPER_PLANE_COUNT = 200;
    public static int BUCKET_MAX_SIZE = 200;
    public static int COUNT_RECENT_OBJECT_TO_COMPARE = 1000;
    int m = 1000; // num features
    int r = 4; // num hash functions in a band
    int b = 4; // num bands

    private JavaPairRDD<Long, SparseVector>  data;

    public LSH(JavaPairRDD<Long, SparseVector> data, int maxIndex, int numRows, int numBands) {
        this.data = data;
        this.r = numRows;
        this.b = numBands;
    }

    public LSHModel run(JavaSparkContext sc) {
        JavaPairRDD<Long, SparseVector> dataRDD = data.cache();
        //LSHModel model = new LSHModel(m, r, b);
        Broadcast<LSHModel> model = sc.broadcast(new LSHModel(m, r, b));
        int test = 1;
        model.getValue().getHashFunctions().get(0l);
        JavaPairRDD<Tuple2<Integer, String>, Long> signatureMatix = dataRDD.flatMap(v -> {
            long hashFId = 0l;
            int minhash = 0;
            //((bandId, concat hashcode),vectorid)
            List<Tuple2<Tuple2<Integer, String>, Long>> bandID_HashCode_vector = new ArrayList<Tuple2<Tuple2<Integer, String>, Long>>();
            for (int i = 0; i < b; i++) {
                String concatHashKey = "";
                for (int j = 0; j < r; j++) {
                    hashFId = i * r + j;
                    //System.out.println("hash id " + hashFId);
                    minhash = model.getValue().getHashFunctions().get((long)hashFId).hash(v._2());
                    concatHashKey += minhash;
                    //List hash value cua hashFid
                }
                bandID_HashCode_vector.add(new Tuple2<Tuple2<Integer, String>, Long>(new Tuple2<Integer, String>(i, concatHashKey), v._1()));
            }
            return bandID_HashCode_vector.iterator();
        }).mapToPair(v -> v);
       logger.info("signature count" + signatureMatix.count());
        model.getValue().setSignatureMatix(signatureMatix);

        /// test 
        signatureMatix.groupByKey().repartition(1).saveAsTextFile("data/signatureMatrix.txt");//.filter(x -> Lists.newArrayList(x._2()).size() >= 2).collect()
       
        return model.getValue();
    }

    public double cosineDistance(SparseVector a, SparseVector b) {
        List<Integer> indicesa = Ints.asList(a.indices());
        List<Integer> indicesb = Ints.asList(b.indices());

        List<Integer> intersection = new ArrayList<>(indicesa);
        intersection.retainAll(indicesb);

        double numerator = 0.0d;
        double d1 = 0.0d;
        double d2 = 0.0d;

        for (int i : intersection) {
            numerator += a.values()[i] * b.values()[i];
        }

        for (int i : indicesa) {
            d1 += a.values()[i] * a.values()[i];
        }

        for (int i : indicesb) {
            d2 += b.values()[i] * b.values()[i];
        }

        double cosineSimilarity = numerator / (Math.sqrt(d1) * Math.sqrt(d2));
        return (1 - cosineSimilarity) < 0 ? 0 : (1 - cosineSimilarity);
    }


    public static void main(String[] args) {
        Map<Integer, Integer> map = new HashMap<>();
        map.put(1, 1);
        map.put(2, 2);
        map.put(3, 3);

        Map<Integer, Integer> map2 = new HashMap<>();
        map2.put(4, 4);
        map2.put(5, 5);
        map2.put(6, 6);
        List<Tuple2<Integer, Integer>> list = map2.entrySet().stream().map(v -> {
            return new Tuple2(v.getKey(), v.getKey());
        }).collect(Collectors.toList());
       // System.out.println(sc.parallelize(list).groupBy(d -> d._2()));

    }
}
