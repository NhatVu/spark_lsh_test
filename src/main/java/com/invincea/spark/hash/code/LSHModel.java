//package com.invincea.spark.hash.code;
//
//import java.io.Serializable;
//import java.util.ArrayList;
//import java.util.List;
//import java.util.Map;
//import java.util.stream.Collectors;
//import java.util.stream.IntStream;
//
//import org.apache.spark.api.java.JavaPairRDD;
//import org.apache.spark.api.java.JavaRDD;
//import org.apache.spark.mllib.linalg.SparseVector;
//
//import scala.Tuple2;
//
//public class LSHModel implements Serializable{
//	/**
//	 * 
//	 */
//	private static final long serialVersionUID = -8049113234507070834L;
//	private int p;
//	private int m;
//	private int numRows;
//	Map<Hasher, Integer> mHashFunctions;
//	public LSHModel(int p, int m, int numRows) {
//		this.p = p;
//		this.m = m;
//		this.numRows = numRows;
//		
//		List<Hasher> _hashFunctions = new ArrayList<Hasher>();
//		for (int i = 0; i < this.numRows; i++) {
//			_hashFunctions.add(Hasher.create(p, m));
//		}
//		//JavaRDD<Hasher> sparkObject = 
//		mHashFunctions = IntStream.range(0, _hashFunctions.size()).boxed().collect(Collectors.toMap(_hashFunctions::get, i -> i));
//	}
//
//	/** generate rows hash functions */
//
//	public Map<Integer,Hasher> hashFunctions() {
//		//return mHashFunctions;
//		return null;
//	}
//
//	/** the signature matrix with (hashFunctions.size signatures) */
//	private JavaRDD<List<Integer>> signatureMatrix = null;
//
//	/** the "bands" ((hash of List, band#), row#) */
//	public JavaPairRDD<Tuple2<Integer,Integer>,Iterable<Long>> bands = null;
//	  //var bands:JavaRDD[((Int,Int),Iterable[Long])]=null
//
//	/** (vector id, cluster id) */
//	public JavaPairRDD<Long, Long> vector_cluster = null;
//	//  var vector_cluster:JavaRDD[(Long,Long)]=null
//
//	/** (cluster id, vector id) */
//	public JavaPairRDD<Long, Long> cluster_vector = null;
//	  //var cluster_vector:JavaRDD[(Long,Long)]=null
//
//	/** (cluster id, List(Vector) */
//	public JavaPairRDD<Long, Iterable<SparseVector>> clusters = null;
//	  //var clusters:JavaRDD[(Long,Iterable[SparseVector])]=null
//
//	/** jaccard cluster scores */
//	public JavaRDD<Map<Long, Double>> scores = null;
//	//  var scores:JavaRDD[(Long,Double)]=null
//	
//
//	/** filter out scores below threshold. this is an optional step.*/
////	  def filter(score : Double) : LSHModel = {
////	    
////	    val scores_filtered = scores.filter(x => x._2 > score)
////	    val clusters_filtered = scores_filtered.join(clusters).map(x => (x._1, x._2._2))
////	    val cluster_vector_filtered = scores_filtered.join(cluster_vector).map(x => (x._1, x._2._2))
////	    scores = scores_filtered
////	    clusters = clusters_filtered
////	    cluster_vector = cluster_vector_filtered
////	    vector_cluster = cluster_vector.map(x => x.swap)
////	    this
////	  }
//}
