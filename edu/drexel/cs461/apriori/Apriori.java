package edu.drexel.cs461.apriori;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.GroupedData;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/**
 * 
 * @author Frequent itemset mining using Apache Spark SQL.
 *
 */
public final class Apriori {

	private static JavaSparkContext sparkContext;
	private static SQLContext sqlContext;

	/**
	 * Set up Spark and SQL contexts.
	 */
	private static void init(String master, int numReducers) {

		Logger.getRootLogger().setLevel(Level.WARN);

		SparkConf sparkConf = new SparkConf().setAppName("Apriori")
				.setMaster(master) // Master URL to connect to (
									// spark://[master]:port)
				.set("spark.sql.shuffle.partitions", "" + numReducers);

		sparkContext = new JavaSparkContext(sparkConf);
		sqlContext = new org.apache.spark.sql.SQLContext(sparkContext);
	}

	/**
	 * 
	 * @param inFileName
	 * 
	 * @return
	 */
	private static DataFrame initXact(String inFileName) {

		// read in the transactions file
		JavaRDD<String> xactRDD = sparkContext.textFile(inFileName);

		// establish the schema: XACT (tid: string, item: int)
		List<StructField> fields = new ArrayList<StructField>();
		fields.add(DataTypes.createStructField("tid", DataTypes.StringType,
				true));
		fields.add(DataTypes.createStructField("item", DataTypes.IntegerType,
				true));
		StructType xactSchema = DataTypes.createStructType(fields);

		JavaRDD<Row> rowRDD = xactRDD.map(new Function<String, Row>() {
			static final long serialVersionUID = 42L;

			public Row call(String record) throws Exception {
				String[] fields = record.split("\t");
				return RowFactory.create(fields[0],
						Integer.parseInt(fields[1].trim()));
			}
		});

		// create DataFrame from xactRDD, with the specified schema
		return sqlContext.createDataFrame(rowRDD, xactSchema);
	}

	private static void saveOutput(DataFrame df, String outDir, String outFile)
			throws IOException {

		File outF = new File(outDir);
		outF.mkdirs();
		BufferedWriter outFP = new BufferedWriter(new FileWriter(outDir + "/"
				+ outFile));

		List<Row> rows = df.toJavaRDD().collect();
		for (Row r : rows) {
			outFP.write(r.toString() + "\n");
		}

		outFP.close();

	}

	public static ArrayList<Integer> frequentItems(DataFrame df, double thresh) {
		List<Row> rows = df.toJavaRDD().collect();

		// Get list of all of the items
		ArrayList<Integer> items = new ArrayList<Integer>();
		for (Row row : rows) {
			items.add(row.getInt(1));
		}

		ArrayList<Integer> frequentItems = new ArrayList<Integer>();
		double num_items = items.size(); // Full length of item set
		// Loop through all items
		while (!items.isEmpty()) {
			int item = items.get(0);
			int index = 1; // Index for the sublist, basically just an iterator
			double count = 1; // Occurrences of the item

			// Loop through all items after current
			while (index < items.size()) {
				// Increase count and remove item if its the same as items[0]
				if (item == items.get(index)) {
					count++;
					items.remove(index);
				}
				// Increment to skip over the item otherwise
				else {
					index++;
				}
			}
			// Remove the 'master' item for this iteration
			items.remove(0);
			// Add to frequent 1-item sets if occurence over threshold
			// Not sure if this should be > thresh or >= thresh
			if ((count / num_items) >= thresh) {
				frequentItems.add(item);
			}
		}
		return frequentItems;

	}

	public static void main(String[] args) throws Exception {

		if (args.length != 5) {
			System.err
					.println("Usage: Apriori <inFile> <support> <outDir> <master> <numReducers>");
			System.exit(1);
		}

		String inFileName = args[0].trim();
		double thresh = Double.parseDouble(args[1].trim());
		String outDirName = args[2].trim();
		String master = args[3].trim();
		int numReducers = Integer.parseInt(args[4].trim());

		Apriori.init(master, numReducers);
		DataFrame xact = Apriori.initXact(inFileName);

		ArrayList<Integer> frequentItems = frequentItems(xact, thresh);

		// TESTING
		// ----------------------------------------------------------------------------------------------------------
		// xact.registerTempTable("transactions");
		xact.printSchema();
		// System.out.println(frequentItems);
		// [3, 32, 36, 38, 39, 41, 48, 79] @ .02 thresh

		// GroupedData test = xact.groupBy("item");
		// xact.select("item").show();
		// System.out.println(test.count());
		// xact.groupBy("item").count().show();

		// Column col = xact.col("item");		
		
		// ----------------------------------------------------------------------------------------------------------

		// compute frequent pairs (itemsets of size 2), output them to a file
		// your code goes here
		// Get all frequent 1-item sets
		// Set C_k to the previous entry in frequent itemsets (by loop)
		// Loop through whole database as t
		// Loop through every entry in C_k as c
		// check if candidate c is in t (is this a table or a row of the db? a
		// relation?)
		// If yes, add to the "count" on candidate c
		// before loop back to C_k increment, set current F_k to candidates
		// which meat threshold by c.count/n >= thresh
		// LOOP TO SET C_K

		// Make copy of the dataframe to avoid messing with triples calculation
		// (not necessary?)
		DataFrame rows_frame = xact;
		HashMap<ArrayList<Integer>, Integer> pairs = new HashMap<ArrayList<Integer>, Integer>();
		for (int k = 1; k < frequentItems.size(); k++) {
			int candidate = frequentItems.get(k - 1);
			// Want to find every occurence of candidate in xact
			// Loop through just those PIDs that contain candidate
			List<Row> candidate_rows = xact.filter("item = " + candidate)
					.toJavaRDD().collect();
			// Remove all occurences of candidate from dataframe to avoid
			// duplicates
			rows_frame = rows_frame.filter("item != " + candidate);
			for (Row c : candidate_rows) {
				// Get all of the rows with matching tid
				List<Row> search_rows = rows_frame.filter(rows_frame.col("tid")
						.equalTo(c.getString(0))).toJavaRDD()
						.collect();
				for (Row r : search_rows) {
					// Add current pair of items to array list for mapping
					ArrayList<Integer> current_pair = new ArrayList<Integer>();
					current_pair.add(c.getInt(1));
					current_pair.add(r.getInt(1));
					// Increment mapped count if the pair exists
					if (pairs.containsKey(current_pair)) {
						int current_count = pairs.get(current_pair);
						pairs.put(current_pair, current_count + 1);
					}
					// Initialize the pair at count 1 if it isn't found
					else {
						pairs.put(current_pair, 1);
					}

				}
			}
		}
		// CREATE NEW DATAFRAME
		// -----------------------------------------------------------------------------------
//		Create the fields
		List<StructField> fields = new ArrayList<StructField>();
		fields.add(DataTypes.createStructField("item1", DataTypes.IntegerType,
				true));
		fields.add(DataTypes.createStructField("item2", DataTypes.IntegerType,
				true));
		fields.add(DataTypes.createStructField("count", DataTypes.IntegerType,
				true));
		StructType xactSchema = DataTypes.createStructType(fields);
		
//		Calculate pairs based on thresh & write to temp file
//		For lack of a better option... I'm so sorry
		double num_items = xact.toJavaRDD().collect().size();		
		String temp_file = "temp_pairs";
		PrintWriter writer = new PrintWriter(temp_file, "UTF-8");
		for (Entry<ArrayList<Integer>, Integer> entry : pairs.entrySet()) {
			if ((entry.getValue() / num_items) >= thresh) {
				// Add to string list here
				System.out.println("item1:\t" + entry.getKey().get(0) + "\titem2:\t" + entry.getKey().get(1) + "\tcount:\t" + entry.getValue()); 
				writer.println(entry.getKey().get(0) + "\t" + entry.getKey().get(1) + "\t" + entry.getValue());
			}
		}
		writer.close();
		
//		Initialize dataframe from temp file
		JavaRDD<String> xactRDD = sparkContext.textFile(temp_file);
		JavaRDD<Row> rowRDD = xactRDD.map(new Function<String, Row>() {
			static final long serialVersionUID = 42L;

			public Row call(String record) throws Exception {
				String[] fields = record.split("\t");
				return RowFactory.create(Integer.parseInt(fields[0].trim()),
						Integer.parseInt(fields[1].trim()),
						Integer.parseInt(fields[2].trim()));
			}
		});
//		Build the dataframe
		DataFrame frequentPairs = sqlContext.createDataFrame(rowRDD, xactSchema);
		// -----------------------------------------------------------------------------------

		try {
			Apriori.saveOutput(frequentPairs, outDirName + "/" + thresh,
					"pairs");
		} catch (IOException ioe) {
			System.out.println("Cound not output pairs " + ioe.toString());
		}
		
		System.out.println("Outputted Pairs");

		// compute frequent triples (itemsets of size 3), output them to a file
		DataFrame frequentTriples = null;
		// your code goes here

		try {
			Apriori.saveOutput(frequentTriples, outDirName + "/" + thresh,
					"triples");
		} catch (IOException ioe) {
			System.out.println("Cound not output triples " + ioe.toString());
		}
		System.out.println("Outputted Triples");
		sparkContext.stop();

	}
}
