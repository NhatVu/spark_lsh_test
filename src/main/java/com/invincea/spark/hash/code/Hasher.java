//package com.invincea.spark.hash.code;
//
//import java.util.ArrayList;
//import java.util.Arrays;
//import java.util.HashMap;
//import java.util.HashSet;
//import java.util.List;
//import java.util.Map;
//import java.util.Random;
//import java.util.Set;
//import java.util.stream.Collectors;
//
//import org.apache.spark.mllib.linalg.SparseVector;
//
//import com.google.common.primitives.Ints;
//
//import breeze.linalg.eig;
//import breeze.linalg.scaleAdd;
//import scala.annotation.meta.param;
//import scala.reflect.internal.Trees.New;
//
///**
// * simple hashing function. defined by ints a, b, p, m 
// * where a and b are seeds with a > 0.
// * p is a prime number, >= u (largest item in the universe)
// * m is the number of hash bins, number of buckets 
// */
//public class Hasher {
//	private int a ;
//	private int b;
//	private int m;
//	private int p;
//	
//	private Hasher(int a, int b, int p, int m) {
//		this.a = a;
//		this.b = b;
//		this.p = p;
//		this.m = m;
//	}
//
//	/** create a new instance providing p and m. a and b random numbers mod p */
//	static public Hasher create(int p, int m) {
//		return new Hasher(a(p), b(p), p, m);
//	}
//	
//	public int hash(int x){
//	    return ((int) (((long)a*x) + b) % p ) % m;
//	  }
//
//	 public int minhhash(SparseVector v) {
//	    // minhash value cho vector cột. 
//		 int[] ints = v.indices();
//		 List<Integer> indices = new ArrayList<Integer>();
//		 for (int index = 0; index < ints.length; index++)
//		 {
//			 indices.add(ints[index]);
//		 }
//		 return indices.stream().map(i -> hash(i)).min(Integer::compare).get();
//	  }
//
//	/** create a seed "a" */
//	static private int a(int p) {
//		int r = new Random().nextInt(p);
//		if (r == 0)
//			r = a(p);
//		return r;
//	}
//
//	/** create a seed "b" */
//	static private int b(int p) {
//		return new Random().nextInt(p);
//	}
//	
//	@Override
//	public String toString() {
//		// TODO Auto-generated method stub
//		return "(" + a + ", " + b + ")";
//	}
//	public static void main(String[] args) {
//		 List<Integer> x = new ArrayList<Integer>();
//		 x.add(4);
//		 x.add(3);
//		 x.add(5);
//		 
//		 List<Integer> y = new ArrayList<Integer>();
//		 y.add(4);
//		 y.add(3);
//		 y.add(5);
//		 System.out.println(y.hashCode() == x.hashCode());
//		 System.out.println(y.iterator().hashCode() == x.iterator().hashCode());
//	}
//}
