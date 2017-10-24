//package com.invincea.spark.hash.code;
//
//import java.io.Serializable;
//import java.util.ArrayList;
//import java.util.Arrays;
//import java.util.Collections;
//import java.util.List;
//import java.util.stream.Collectors;
//
//import org.apache.spark.api.java.JavaPairRDD;
//import org.apache.spark.mllib.linalg.SparseVector;
//import org.apache.spark.rdd.RDD;
//
//import com.google.common.collect.Lists;
//import com.google.common.primitives.Ints;
//
//import breeze.signal.iFourierShift;
//import scala.Tuple2;
//
//public class LSH implements Serializable {
//	private RDD<SparseVector> data;
//	private int p; // a prime number > the largest vector index
//	private int m; // the number of "bins" to hash data into. Smaller numbers increase collisions
//	private int numRows; //the total number of times to minhash a vector. //Nhat: rows of signature matrix 
//	private int numBands;//how many times to chop numRows. Each band will have numRows/numBand hash signatures. The larger number of elements the higher confidence in vector similarity.
//	private int minClusterSize;
//
//	public LSH(RDD<SparseVector> data, int p, int m, int numRows, int numBands, int minClusterSize) {
//		this.data = data;
//		this.p = p;
//		this.m = m;
//		this.numRows = numRows;
//		this.numBands = numBands;
//		this.minClusterSize = minClusterSize;
//	}
//
//	/**
//	 * 
//	 */
//	private static final long serialVersionUID = -2457124881330197554L;
//
//	/** run LSH using the constructor parameters */
//	public LSHModel run() {
//
//		// create a new model object
//		LSHModel model = new LSHModel(p, m, numRows);
//
//		// preserve vector index
//		// (SparseVector,Long)
//		JavaPairRDD<SparseVector, Long> zdata = data.zipWithIndex().toJavaRDD()
//				.mapToPair(v -> new Tuple2<SparseVector, Long>(v._1(), (Long) v._2())).cache();
//
//		// compute signatures from matrix
//		// - hash each vector <numRows> times
//		// - position hashes into bands. we'll later group these signature bins and has
//		// them as well
//		// (// nhật each hash function)this gives us ((vector id, band id), minhash)
//		// v._2: id của vector
//		// h._2 % numBands : id của band
//		// v: vector user 1, user 2
//		// var signatures =
////			JavaPairRDD<Tuple2<Long, Integer>, Integer> signatures=
////
////	zdata
////				.flatMap(v -> model.hashFunctions().entrySet().stream().map(h ->
////				// Entry<Hasher,Integer>
////				new Tuple2<Tuple2<Long, Integer>, Integer>(new Tuple2<Long, Integer>(v._2(), h.getValue() % numBands),
////						h.getKey().minhhash(v._1()))).collect(Collectors.toList()))
////				//.mapToPair(v -> new Tuple2<Tuple2<Long, Integer>, Integer>(v._1(), v._2()));
////				.mapToPair(v -> v);
//	//	JavaPairRDD<Tuple2<Long, Integer>, Integer> signatures=
//				
//				zdata.flatMap(v -> {
//					int hashFId = 0;
//					int minhash = 0;
//					//((bandId, concat hashcode),vectorid)
//					List<Tuple2<Tuple2<Integer,String>, Long>> bandID_HashCode_vector = new ArrayList<Tuple2<Tuple2<Integer,String>, Long>>();
//					for(int i = 0; i < numBands; i++) {
//						String concatHashKey = "";
//						for(int j = 0; j < numRows; j++) {
//							hashFId = i * numRows + j;
//							minhash = model.hashFunctions().get(hashFId).minhhash(v._1());
//							concatHashKey += minhash;
//							//List hash value cua hashFid 
//						}
//						bandID_HashCode_vector.add(new Tuple2<Tuple2<Integer,String>, Long>(new Tuple2<Integer, String>(i, concatHashKey),v._2()));
//					}
//					return bandID_HashCode_vector;
//				});
//
//		// reorganize data for shuffle
//		// groupByKey gives us items that hash together in the same band //
//		// signatures.sort
//		model.bands = signatures.groupByKey() // group vector id khi trung toan bo minhash trong 1 band
//				.cache();
//
//		//
//		// //we only want groups of size >= <minClusterSize>
//		// (input: (bandId, concat_hashcode), [vectorId,...])
//		// output: (vector id, cluster id)http://marketplace.eclipse.org/marketplace-client-intro?mpc_install=421
//		model.vector_cluster = model.bands.filter(x -> Lists.newArrayList(x._2()).size() >= minClusterSize).map(x -> {
//			List<Long> list = Lists.newArrayList(x._2());
//			Collections.sort(list);
//			return list;
//		}).distinct().zipWithIndex()
//				.flatMap(x -> x._1().stream().map(y -> new Tuple2<>(y, x._2())).collect(Collectors.toList()))
//				.mapToPair(x -> x);
//
//		//
//		// //(cluster id, vector id)
//		model.cluster_vector = model.vector_cluster.mapToPair(x -> x.swap());
//		//
//		// //(cluster id, List(vector))
//		model.clusters = zdata.mapToPair(x -> x.swap()).join(model.vector_cluster)
//				.mapToPair(x -> new Tuple2<>(x._2()._2(), x._2()._1())).groupByKey().cache();
//
//		//
//		// //compute the jaccard similarity of each cluster
//		// model.scores = model.clusters.map(row => (row._1,
//		// jaccard(row._2.toList))).cache()
//
//		return model;
//	}
//	
//	  
//	  /** compute a single vector against an existing model */
////	  public JavaPairRDD<Long, SparseVector> compute(SparseVector data,LSHModel model, Double minScore) {
////	    return model.clusters.map(x => (x._1, x._2++List(data))).filter(x => jaccard(x._2.toList) >= minScore);
////	     
////	  }
//	  
//	  /** compute jaccard between two vectors */
//	  public double jaccard(SparseVector a, SparseVector b) {
//	    List<Integer> al = Ints.asList(a.indices());
//	    List<Integer> bl = Ints.asList(b.indices());
//	    al.retainAll(bl);
//	    int intersection = al.size();
//	    al = Ints.asList(a.indices());
//	    al.addAll(bl);
//	    int union = (int) al.stream().distinct().count();
//	    return intersection / (double)union;
//	  }
//	  
//	  /** compute jaccard similarity over a list of vectors */
////	  public double jaccard(List<SparseVector> l){
////	    l.foldLeft(l(0).indices.toList)((a1, b1) => a1.intersect(b1.indices.toList.asInstanceOf[List[Nothing]])).size / 
////	    l.foldLeft(List())((a1, b1) => a1.union(b1.indices.toList.asInstanceOf[List[Nothing]])).distinct.size.doubleValue
////	  }  
//}
