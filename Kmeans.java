package com.eecs476;
import java.io.*;
import java.util.HashSet;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
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
import java.io.FileWriter;   // Import the FileWriter class
import java.io.IOException;

public class Kmeans {
	public static class MyMapper
            extends Mapper<LongWritable, Text, Text, Text> {
        ArrayList<ArrayList<Double>> centroid_list = new ArrayList<ArrayList<Double>>();
        // read from cache
        public void setup(Context context) throws IOException, InterruptedException {
            URI[] cacheFiles = context.getCacheFiles();
            if (cacheFiles != null && cacheFiles.length > 0) {
                try {
                	// read in centroids
                    String line = "";
                    FileSystem fs = FileSystem.get(context.getConfiguration());
                    int index = cacheFiles.length - 1;
                    // System.out.println(index);
                    Path f1Path = new Path(cacheFiles[index].toString()); // read cache, different from other k - 1 iterations
                    // System.out.println(f1Path);
                    // read prev item list
                    BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(f1Path)));
                    // System.out.println("Opened chache file");
                    while ((line = reader.readLine()) != null) {
                        line = line.trim();
                        String[] arr = line.split(",");
                        ArrayList<Double> dl_arr = HelperClass_Kmeans.convertPoint(arr);
                        centroid_list.add(dl_arr);
                    }
                }
                catch (Exception e) {
                    System.out.println("Unable to read the File");
                    System.exit(1);
                }
            }
        }
        @Override
        public void map(LongWritable key, Text value, Context context
        ) throws IOException, InterruptedException {
        	Text centroid_text = new Text();
        	Configuration conf = context.getConfiguration();
            int norm = conf.getInt("norm", -1);
            String str = value.toString();
            str = str.trim();
            String[] arrOfPoint = str.split(",");
            ArrayList<Double> dl = HelperClass_Kmeans.convertPoint(arrOfPoint);
            ArrayList<Double> min_centroid = new ArrayList<Double>();
            Double min_distance = Double.MAX_VALUE;
            for (ArrayList<Double> centroid : centroid_list) {
            	Double temp = HelperClass_Kmeans.calculate_distance(dl, centroid, norm);
            	if (temp < min_distance) {
            		min_centroid = centroid;
            		min_distance = temp;
            	}
            } 
            String centroid_str = HelperClass_Kmeans.convertToString(min_centroid);
            centroid_text.set(centroid_str);
            context.write(centroid_text, value);
        }
    }
    public static class MyMapperK
            extends Mapper<LongWritable, Text, Text, Text> {
        ArrayList<ArrayList<Double>> centroid_list = new ArrayList<ArrayList<Double>>();
        // read from cache
        public void setup(Context context) throws IOException, InterruptedException {
            URI[] cacheFiles = context.getCacheFiles();
            if (cacheFiles != null && cacheFiles.length > 0) {
                try {
                	// read in centroids
                    String line = "";
                    FileSystem fs = FileSystem.get(context.getConfiguration());
                    int index = cacheFiles.length - 1;
                    // System.out.println(index);
                    Path f1Path = new Path(cacheFiles[index].toString() + "/part-r-00000"); // read cache, using intermediate outputs
                    // System.out.println(f1Path);
                    // read prev item list
                    BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(f1Path)));
                    // System.out.println("Opened chache file");
                    while ((line = reader.readLine()) != null) {
                        line = line.trim();
                        String[] arr = line.split(",");
                        ArrayList<Double> dl_arr = HelperClass_Kmeans.convertPoint(arr);
                        centroid_list.add(dl_arr);
                    }
                }
                catch (Exception e) {
                    System.out.println("Unable to read the File");
                    System.exit(1);
                }
            }
        }
        @Override
        public void map(LongWritable key, Text value, Context context
        ) throws IOException, InterruptedException {
        	Text centroid_text = new Text();
        	Configuration conf = context.getConfiguration();
            int norm = conf.getInt("norm", -1);
            String str = value.toString();
            str = str.trim();
            String[] arrOfPoint = str.split(",");
            ArrayList<Double> dl = HelperClass_Kmeans.convertPoint(arrOfPoint);
            ArrayList<Double> min_centroid = new ArrayList<Double>();
            Double min_distance = Double.MAX_VALUE;
            for (ArrayList<Double> centroid : centroid_list) {
            	Double temp = HelperClass_Kmeans.calculate_distance(dl, centroid, norm);
            	if (temp < min_distance) {
            		min_centroid = centroid;
            		min_distance = temp;
            	}
            } 
            String centroid_str = HelperClass_Kmeans.convertToString(min_centroid);
            centroid_text.set(centroid_str);
            context.write(centroid_text, value);
        }
    }

    public static class MyReducerK
            extends Reducer<Text, Text, Text, NullWritable> {
        private Text update = new Text();

        @Override
        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
        	ArrayList<ArrayList<Double>> point_list = new ArrayList<ArrayList<Double>>();
        	Configuration conf = context.getConfiguration();
            int norm = conf.getInt("norm", -1);
            for (Text val : values) {
            	String str = val.toString();
            	String[] arrOfPoint = str.split(",");
            	point_list.add(HelperClass_Kmeans.convertPoint(arrOfPoint));
            }
            // calculate new centroid 
            ArrayList<Double> old_centroid = new ArrayList<Double>();
            String key_str = key.toString();
            String[] arrOfKey = key_str.split(",");
            old_centroid = HelperClass_Kmeans.convertPoint(arrOfKey);
            Double cost = 0.0;
            for (ArrayList<Double> point : point_list) {
            	cost += HelperClass_Kmeans.calculate_distance(old_centroid, point, norm);
            }
            System.out.println(cost);
            BufferedWriter out = null;
			FileWriter fstream = new FileWriter("filename.txt", true); //true tells to append data.
    		out = new BufferedWriter(fstream);
    		out.write("\n" + cost.toString());
            ArrayList<Double> new_centroid = new ArrayList<Double>();
            int dimension = point_list.size();
            for (int i = 0; i < point_list.get(0).size(); i++) {
            	Double temp = 0.0;
            	for (ArrayList<Double>point : point_list) {
            		temp += point.get(i);
            	}
            	new_centroid.add(temp/dimension);
            }
            String update_centroid = HelperClass_Kmeans.convertToString(new_centroid);
            update.set(update_centroid);
            context.write(update,  NullWritable.get());
        }
    }
    private static String outputPath;
    private static String inputPath;
    private static String centroidPath;
    private static int norm;
    private static int k;
    private static int n;
    public static void main(String[] args) {
        System.out.println("Setup your jobs here!");
        for (int i = 0; i < args.length; ++i) {
            if (args[i].equals("-k")) {
                k = Integer.parseInt(args[++i]);
            } else if (args[i].equals("-n")) {
                n = Integer.parseInt(args[++i]);
            } else if (args[i].equals("--inputPath")) {
                inputPath = args[++i];
            } else if (args[i].equals("--outputScheme")) {
                outputPath = args[++i];
            } else if (args[i].equals("--centroidPath")) {
                centroidPath = args[++i];
            } else if (args[i].equals("--norm")) {
                norm = Integer.parseInt(args[++i]);
            } else {
                throw new IllegalArgumentException("Illegal cmd line argument");
            }
        }
        if (outputPath == null || inputPath == null) {
            throw new RuntimeException("Either outputpath or input path are not defined");
        }
        try {
            // think about how to make cache
            Configuration conf = new Configuration();
            conf.set("mapred.textoutputformat.separator", ",");
            conf.set("mapreduce.job.queuename", "eecs476");         // required for this to work on GreatLakes
            conf.setInt("norm", norm); // set s
            //System.out.println("start loop");
            for (int i = 1; i <= n; i++) {
                Job jk = Job.getInstance(conf, "jk");
                //System.out.println("start if");
                if (i == 1) {
                	jk.addCacheFile(new Path(centroidPath).toUri()); // add cache
                }
                else {
                	jk.addCacheFile(new Path(outputPath+String.valueOf(i-1)).toUri()); // add cache
                }
                //System.out.println("end if");
                //System.out.println("set i - 1");
                jk.setJarByClass(Kmeans.class);
                if (i == 1) {
                	jk.setMapperClass(MyMapper.class);
                }
                else {
                	jk.setMapperClass(MyMapperK.class);
                }
                jk.setReducerClass(MyReducerK.class);
                jk.setMapOutputKeyClass(Text.class);
                jk.setMapOutputValueClass(Text.class);
                jk.setOutputKeyClass(Text.class);
                jk.setOutputValueClass(NullWritable.class);
                FileInputFormat.addInputPath(jk, new Path(inputPath));
                FileOutputFormat.setOutputPath(jk, new Path(outputPath + String.valueOf(i)));
                jk.waitForCompletion(true);
            }
        } catch (Exception e) {
            System.err.println(e);
        }
    }

}
