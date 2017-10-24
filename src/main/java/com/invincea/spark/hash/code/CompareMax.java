package com.invincea.spark.hash.code;

import java.io.Serializable;
import java.util.Comparator;

public class CompareMax<Integer> implements Comparator<Integer>,Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	@Override
	public int compare(Integer o1, Integer o2) {
		// TODO Auto-generated method stub
		int a = (int)o1;
		int b = (int)o2;
		return a - b;
	}
	
}
