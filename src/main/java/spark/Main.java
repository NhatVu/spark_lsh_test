package spark;

import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.linalg.SparseVector;
import org.apache.spark.mllib.linalg.Vectors;

import com.invincea.spark.hash.code.CompareMax;

import scala.Tuple2;
import scala.Tuple3;

public class Main {
	private transient Logger logger = Logger.getLogger(Main.class);
	private final transient JavaSparkContext sc ;
	public Main() {
		SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("abc");
		sc =  new JavaSparkContext(conf);
	}

	public void run() {
		String dataFile = "data/ml-1m.data";

		int numRows = 8;
		int numBands = 6;
		JavaRDD<String> data = sc.textFile(dataFile);
		// parse data and create (user, item, rating) tuples
		JavaRDD<Tuple3<Integer, Integer, Double>> ratingsRDD = data.map(line -> line.split("::"))
				.map(elems -> new Tuple3<Integer, Integer, Double>(Integer.valueOf(elems[0]), Integer.valueOf(elems[1]),
						Double.valueOf(elems[2])));

		// list of distinct items
		JavaRDD<Integer> items = ratingsRDD.map(x -> x._2()).distinct();
		System.out.println(items.count());
		// items.max((a, b) -> a-b);
		int maxIndex = items.max(new CompareMax<Integer>()) + 1;

		// user ratings grouped by user_id
		JavaPairRDD<Integer, Iterable<Tuple2<Integer, Double>>> userItemRatings = ratingsRDD.mapToPair(
				x -> new Tuple2<Integer, Tuple2<Integer, Double>>(x._1(), new Tuple2<Integer, Double>(x._2(), x._3())))
				.groupByKey().cache();

		// convert each user's rating to tuple of (user_id, SparseVector_of_ratings)
		JavaPairRDD<Long, SparseVector> sparseVectorData = userItemRatings.mapToPair(
				a -> new Tuple2<Long, SparseVector>(Long.valueOf(a._1()), Vectors.sparse(maxIndex, a._2()).toSparse()));

		// run locality sensitive hashing model with 6 hashTables and 8 hash functions
		LSH lsh = new LSH(sparseVectorData, maxIndex, numRows, numBands);
		LSHModel model = lsh.run(sc);

		// print sample hashed vectors in ((hashTableId#, hashValue), vectorId) format
		logger.info("signatrue matrix take 10");
		model.getSignatureMatix().take(10).forEach(r -> logger.info(r));

		// get the near neighbors of userId: 4587 in the model
		JavaRDD<Long> candList = model.getCandidates(1884);
		logger.info("Number of Candidate Neighbors: " + candList.count());
		logger.info("Candidate List: " + candList.collect().toString());
	}

	public static void main(String[] args) {
		Main main = new Main();
		main.run();
	}

}
