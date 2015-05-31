package edu.drexel.cs461.apriori;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

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
 * @author  
 * Frequent itemset mining using Apache Spark SQL.
 *
 */
public final class Apriori {
    
    private static JavaSparkContext sparkContext;
    private static SQLContext sqlContext;
    
    /**
     * Set up Spark and SQL contexts.
     */
    private static void init (String master, int numReducers) {
	
	Logger.getRootLogger().setLevel(Level.WARN);
	
	SparkConf sparkConf = new SparkConf().setAppName("Apriori")
	    .setMaster(master) // Master URL to connect to ( spark://[master]:port)
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
    private static DataFrame initXact (String inFileName) {
	
	// read in the transactions file
	JavaRDD<String> xactRDD = sparkContext.textFile(inFileName);
	
	// establish the schema: XACT (tid: string, item: int)
	List<StructField> fields = new ArrayList<StructField>();
	fields.add(DataTypes.createStructField("tid", DataTypes.StringType, true));
	fields.add(DataTypes.createStructField("item", DataTypes.IntegerType, true));
	StructType xactSchema = DataTypes.createStructType(fields);

	JavaRDD<Row> rowRDD = xactRDD.map(
					  new Function<String, Row>() {
					      static final long serialVersionUID = 42L;
					      public Row call(String record) throws Exception {
						  String[] fields = record.split("\t");
						  return  RowFactory.create(fields[0], Integer.parseInt(fields[1].trim()));
					      }
					  });

	// create DataFrame from xactRDD, with the specified schema
	return sqlContext.createDataFrame(rowRDD, xactSchema);
    }
    
    private static void saveOutput (DataFrame df, String outDir, String outFile) throws IOException {
	
	File outF = new File(outDir);
        outF.mkdirs();
        BufferedWriter outFP = new BufferedWriter(new FileWriter(outDir + "/" + outFile));
            
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
    		int index = 1; //Index for the sublist, basically just an iterator
    		double count = 1; //Occurrences of the item
    		
    		// Loop through all items after current
    		while(index < items.size())
    		{
    			// Increase count and remove item if its the same as items[0]
    			if(item == items.get(index)) { 
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
    		if ((count/num_items) >= thresh) { frequentItems.add(item); }
    	}
    	return frequentItems;
    	
    }

    public static DataFrame candidateGen(DataFrame df, ArrayList<Integer> frequentItems) {
    	// Candidate Gen Function
    	// 
    	return df;
    }
    
    public static void main(String[] args) throws Exception {

	if (args.length != 5) {
	    System.err.println("Usage: Apriori <inFile> <support> <outDir> <master> <numReducers>");
	    System.exit(1);
	}

	String inFileName = args[0].trim();
	double thresh =  Double.parseDouble(args[1].trim());
	String outDirName = args[2].trim();
	String master = args[3].trim();
	int numReducers = Integer.parseInt(args[4].trim());

	Apriori.init(master, numReducers);
	DataFrame xact = Apriori.initXact(inFileName);
	
	
	ArrayList<Integer> frequentItems = frequentItems(xact, thresh);
	
	
	//TESTING
	// ----------------------------------------------------------------------------------------------------------
//	xact.registerTempTable("transactions");
	xact.printSchema();
//	System.out.println(frequentItems);
//	[3, 32, 36, 38, 39, 41, 48, 79] @ .02 thresh
	
//	List<Row> rows = xact.toJavaRDD().collect();
//	for (Row row : rows) {
//		System.out.println(row.get(0) + "\t" + row.get(1));
//	}
//	System.out.println(rows);
	
	
	
//	DataFrame teenagers = sqlContext.sql("SELECT name FROM parquetFile WHERE age >= 13 AND age <= 19");
//	Above may very well be what I have from 'select'
//	List<Integer> items = xact.map(new Function<Row, Integer>() {
//	  public int call(Row row) {
//	    return row.getString(0);
//	  }
//	}).collect();
	
//	List<Row> rows = sqlContext.sql("SELECT *").toJavaRDD().collect();
//	List<Row> rows = df.toJavaRDD().collect();

//	GroupedData test = xact.groupBy("item");
//	xact.select("item").show();
//	List<Row> rows = test.agg("item");
//	System.out.println(test.count());
//	xact.groupBy("item").count().show();

	//	System.out.println(xact.count());
//	Column col = xact.col("item");
	
	
	// ----------------------------------------------------------------------------------------------------------
	
	// compute frequent pairs (itemsets of size 2), output them to a file
	DataFrame frequentPairs = null;
	// your code goes here

	// MINSUP:
	// User specified support threshold
	// Second user argument, "support"
	// args[1], double 'thresh'
	
	// Data Structure Fields:
	// String tid
	// int Item
	// Need to add 'count'?

	// NECESSARY VARS:
	// T -- Whole dataset
	// F = frequent itemsets
	// C = Candidate key generated at current F (I think its actually just Current Fk - 1)
	// c.count = number of times registered on this candidate key
	// Uk = I have no clue-- I guess its just all of the pairs?
	// minsup = "thresh" -- the minimum support threshold

	// Get all frequent 1-item sets
	// Set C_k to the previous entry in frequent itemsets (by loop)
	// Loop through whole database as t
	// Loop through every entry in C_k as c
	// check if candidate c is in t (is this a table or a row of the db? a relation?)
	// If yes, add to the "count" on candidate c
	// before loop back to C_k increment, set current F_k to candidates which meat threshold by c.count/n >= thresh
	// LOOP TO SET C_K

	
//	THIS CODE SEGMENT IS PROBABLY GARBAGE
//	for(k=2; frequentItems.where($"tid" = k-1) != null; k++) {
//		// Definitely not accurate -- what is the candidate key supposed to be? Just an int? How do I pull that from the DataFrame object?
//		// This is supposed to be everything that matches the candidate key in the dataframe?
//		// Need to loop through this later too...
//		DataFrame candidate_gen = frequentItems.where($"tid" = k-1);
//		for(Object entry : xact) //Object??
//		{
//			for(Object candidate : candidate_gen)
//			{
//				if(entry.contains(candidate)) { entry.count ++; }
//			}
//		}
//		// if(c.count/n >= thresh) { frequentPairs.add()}
//		// This should be adding all of the candidates that are over the threshold to "frequentItems"[k]
//	}	

	
	//OUTLINE OF THE APRIORI 2-ITEM SETS ALGORITH
	// ----------------------------------------------------------------------------------------------------------
	// function APriori(T) <-- T = whole itemset
	// 	F1 = Frequent 1-item sets
	// 	for k=2, Fk-1 != 0, k++:
	// 		Ck = candidate-gen(fk-1) <-- Candidate-gen????
	// 		for transaction t in T:
	// 			for candidate c in Ck:
	// 				if c in t:
	// 					c.count ++
	// 				endif
	// 			enfor
	// 		Fk = {c in Ck or c.count/n >= minsup } <-- Not sure what this section is doing... minsup??
	// 	endfor
	// 	return F = UkFk <-- What is UkFk??
	// ----------------------------------------------------------------------------------------------------------
		
	try {
	    Apriori.saveOutput(frequentPairs, outDirName + "/" + thresh, "pairs");
	} catch (IOException ioe) {
	    System.out.println("Cound not output pairs " + ioe.toString());
	}

	// compute frequent triples (itemsets of size 3), output them to a file
	DataFrame frequentTriples = null;
	// your code goes here
	
	try {
	    Apriori.saveOutput(frequentTriples, outDirName + "/" + thresh, "triples");
	} catch (IOException ioe) {
	    System.out.println("Cound not output triples " + ioe.toString());
	}
	
	sparkContext.stop();
	        
    }
}
