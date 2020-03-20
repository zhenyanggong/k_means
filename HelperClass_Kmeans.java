package com.eecs476;
import java.io.*;
import java.util.HashSet;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import java.io.*;
import java.util.*;
import java.net.URI;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import java.util.ArrayList;
import java.io.IOException;
import java.util.*;
import java.util.List;
import java.lang.*;
import java.util.Arrays;

public class HelperClass_Kmeans {
	public static ArrayList<Double> convertPoint(String[] str_list) {
		// convert text to vector of doubles
		ArrayList<Double> dl = new ArrayList<Double>();
		for (String i : str_list) {
			dl.add(Double.parseDouble(i));
		}
		return dl;
	}
	public static String convertToString(ArrayList<Double> dl) {
		// convert point vector to a string with ','
		String str = "";
		for (Double i : dl) {
			str += String.valueOf(i);
			str += ",";
		}
		if (str.length() > 1) {
			str = str.substring(0, str.length() - 1);
		}
		return str;
	}
	public static Double calculate_distance(ArrayList<Double> point, ArrayList<Double> centroid, int norm) {
		Double distance = 0.0;
		Double sum = 0.0;
		if (norm == 2) {
			for (int i = 0; i < point.size(); i++) {
				sum += ((point.get(i) - centroid.get(i)) * (point.get(i) - centroid.get(i))); 
			}
			distance = Math.sqrt(sum);
		}
		else if (norm == 1) {
			for (int i = 0; i < point.size(); i++) {
				sum += Math.abs(point.get(i) - centroid.get(i));
			}
			distance = sum;
		}
		else {
			System.out.print("Dsitance error!");
		}
		// calculate distance by norm
		return distance;
	}
}