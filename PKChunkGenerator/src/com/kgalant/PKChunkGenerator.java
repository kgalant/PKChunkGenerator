package com.kgalant;

import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

public class PKChunkGenerator {

	final static String base62Chars = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";

	private static CommandLine commandLine = null;
	private static Options options = new Options();
	
	private static boolean isStrict = false;
	private static boolean isVerbose = false;
	
	private static final int DEFAULTCHUNKSIZE = 10000000;
	private static final String DEFAULTFILENAME = "output.txt";

	public static void main(String[] args) throws RemoteException, Exception {

		setupOptions();
		parseCommandLine(args);

		if(commandLine.hasOption(HELPSHORT) ) {
			printHelp();
			System.exit(0);
		}

		if(commandLine.hasOption(CONVERTBASE62SHORT)) {
			handleBase62Conversion();
			System.exit(0);
		}

		if(commandLine.hasOption(CONVERTBASE10SHORT)) {
			handleBase10Conversion();
			System.exit(0);
		}

		if(commandLine.hasOption(DISTANCESHORT)) {
			handleDistanceCalculation();
			System.exit(0);
		}

		if(commandLine.hasOption(ADDSHORT)) {
			handleAddCalculation();
			System.exit(0);
		}
		if(commandLine.hasOption(GENERATECHUNKSHORT)) {
			handleGenerateChunks();
			System.exit(0);
		}

		// if we're still here, print the help and exit

		printHelp();
		System.exit(0);

		//		List<String> queryList = new ArrayList<String>();
		//		long start = getBase10Number("02i0k00000GThePAAT".substring(6,15));
		//		long end = getBase10Number("02i0k00000GYLoiAAH".substring(6,15));
		//
		//		generateBase62String(getBase10Number("02i0k00000KEZseAAH".substring(6,15)),400);
		//		queryList = getExportQueriesForSObject(args[0], args[1], (Integer.parseInt(args[2])), args[3], args[4], args.length < 6 ? "" : args[5]);

	}

	private static void handleGenerateChunks() {
		// check if we have all needed parameters
		
		int chunkSize = 10000000;
		if (commandLine.hasOption(CHUNKSIZESHORT)) {
			String chunksizeParam = commandLine.getOptionValue(CHUNKSIZESHORT);
			if (chunksizeParam != null && chunksizeParam.length() > 0) {
				try {
					chunkSize = Integer.parseInt(chunksizeParam);
				} catch (NumberFormatException e) {
					System.out.println("Tried to convert input value " + chunksizeParam + " to a number, but failed. Cannot proceed");
					e.printStackTrace();
					System.exit(1);
				}
			}
		}
		// get field names
		String fieldNames = "Id";
		if (commandLine.getOptionValue(FIELDNAMESHORT) != null && commandLine.getOptionValue(FIELDNAMESHORT).length() > 0) {
			fieldNames = commandLine.getOptionValue(FIELDNAMESHORT);
		}
		// get object name
		String objectName = commandLine.getOptionValue(OBJECTNAMESHORT);
		if (objectName == null || objectName.length() < 3) {
			System.out.println("Invalid object name provided. Got: " + objectName + ". Cannot proceed");
			System.exit(1);
		}
		// get start and end IDs
		String startID = commandLine.getOptionValue(STARTIDSHORT);
		String endID = commandLine.getOptionValue(ENDIDSHORT);
		long startIDBase10 = decodeSalesforceID(startID);
		long endIDBase10 = decodeSalesforceID(endID);
		if (isStrict && 
				(
						(startIDBase10 > endIDBase10) || 
						(!(startID.substring(6,  15).equals(endID.substring(6,  15))))
				) 
			) {
			System.out.println("Converted start ID greater than end ID or prefixes don't match, and strict option is in force. Cannot proceed");
			System.out.println("Start ID received: " + startID + " converted: " + startIDBase10 + " End ID: " + endID + 
					" converted: " + endIDBase10);
			System.exit(1);
		}
		// regenerate chunk size if no chunksize parameter is provided
		if (!commandLine.hasOption(CHUNKSIZESHORT) && Math.abs(endIDBase10-startIDBase10) < 10 * DEFAULTCHUNKSIZE) {
			chunkSize = (int) Math.round(((Math.abs((float)(endIDBase10-startIDBase10)))/10));
		}
		// now figure out the start / end in case they were flipped
		if (endIDBase10 < startIDBase10) {
			long temp = startIDBase10;
			startIDBase10 = endIDBase10;
			endIDBase10 = temp;
		}
		// get filename parameter
		String filename = DEFAULTFILENAME;
		if (commandLine.getOptionValue(OUTPUTFILESHORT) != null && commandLine.getOptionValue(OUTPUTFILESHORT).length() > 0) {
			filename = commandLine.getOptionValue(OUTPUTFILESHORT);
		}
		// get gt parameter
		boolean gt = false;
		
		if (commandLine.hasOption(RANGESTARTSHORT)) {
			gt = true;
		}
		
		// now generate the actual queries
		
		List<String> queryList = new ArrayList<String>();
	    Integer batchCount = 1;
	    String prefix = ((String)startID).substring(0,6);
	    boolean firstBatch = true;
	    for(long recordsProcessed = startIDBase10; recordsProcessed < endIDBase10; recordsProcessed += chunkSize) {
	    		String startOperator = ">=";
	        long numericEndRange = recordsProcessed + chunkSize - 1;
	        if (numericEndRange > endIDBase10) numericEndRange = endIDBase10;
	    		long numericStartRange = recordsProcessed;
	    		if (firstBatch) {
	    			if (gt) {
	    				startOperator = ">";
	    			}
	    			firstBatch = false;
	    		}
	        String startRangeId = encodeBase10(numericStartRange, 9);
	        String endRangeId = encodeBase10(numericEndRange, 9);
//	        System.out.println("numericStartRange: " + numericStartRange + " ID:" + startRangeId);
//    			System.out.println("numericEndRange: " + numericEndRange + " ID:" + endRangeId);
	        String soql = "SELECT " + fieldNames + " FROM " + objectName + " WHERE Id " + 
	        		startOperator + " '" + prefix + startRangeId + 
	        		"' AND Id <= '" + prefix + endRangeId + "' ORDER BY Id asc limit " + chunkSize;
	        System.out.println(soql);
	        queryList.add(soql);
	        
	        batchCount++;
	    }
	    
	    // now generate the file
	    
		BufferedWriter writer = null;
		try {
			writer = new BufferedWriter(new OutputStreamWriter(
					new FileOutputStream(filename), StandardCharsets.ISO_8859_1));
			for (String s : queryList) {
				writer.write(s);
				writer.newLine();
			}

		} catch (IOException e) {
			System.out.println("Something failed when writing file. See stack trace below.");
			e.printStackTrace();
			System.exit(1);
		} finally {
			try {
				if (writer != null)
					writer.close();
			} catch (IOException e) {
				System.out.println("Something failed when closing output stream. See stack trace below.");
				e.printStackTrace();
				System.exit(1);
			}
		}

	}

	private static void handleAddCalculation() {
		String paramsToCalculate = commandLine.getOptionValue(ADDSHORT);
		String[] params = paramsToCalculate.split(",");
		if (params.length != 2) {
			System.out.println("Need exactly two commaseparated parameters: one 15 or 18 character ID, one integer. Received: " + paramsToCalculate);
			System.exit(1);	
		}
		if (params[0] == null || !(params[0].length() == 18 || params[0].length() == 15)) {
			System.out.println("Parameter 1 must be a 15 or 18 character ID. Received: " + params[0]);
			System.exit(1);	
		}
		long valueToAdd = 0;
		try {
			valueToAdd = Long.parseLong(params[1]);
		} catch (NumberFormatException e) {
			System.out.println("Tried to convert input value " + params[1] + " to a number, but failed. Cannot proceed");
			e.printStackTrace();
			System.exit(1);
		}
		long baseValue = getBase10Number(params[0].substring(6, 15));
		long totalValue = baseValue + valueToAdd;
		String resultingId = params[0].substring(0, 6) + encodeBase10(totalValue, 9);
		if (isVerbose) {
			System.out.print("Starting point: " + params[0]);
			System.out.print(" Distance requested: " + params[1]);
			System.out.print(" End ID: ");
		}
		System.out.println(resultingId);
		
	}

	private static void handleDistanceCalculation() {
		String IDsToCalculate = commandLine.getOptionValue(DISTANCESHORT);
		String[] IDs = IDsToCalculate.split(",");
		if (IDs.length != 2 || 
				IDs[0] == null || !(IDs[0].length() == 18 || IDs[0].length() == 15) ||
				IDs[1] == null || !(IDs[1].length() == 18 || IDs[1].length() == 15)) {
			System.out.println("Need exactly two commaseparated IDs of 15 or 18 characters to calculate distance. Received: " + IDsToCalculate);
			System.exit(1);
		} else if (!(IDs[0].substring(0, 6).equals(IDs[1].substring(0, 6))) && isStrict) {
			System.out.println("Warning: prefixes not identical and strict option in effect. Cannot continue.");
			System.exit(1);
		} else {
			long base10Number1 = getBase10Number(IDs[0].substring(6, 15));
			long base10Number2 = getBase10Number(IDs[1].substring(6, 15));
			long difference = Math.abs(base10Number2-base10Number1);
			if (isStrict && base10Number1 > base10Number2) {
				System.out.println("Warning: first ID greater than second ID and strict option in effect. Cannot continue.");
				System.exit(1);
			}
			if (isVerbose) {
				System.out.print("Distance: ");
			}
			System.out.println(difference);
			if (isVerbose) {
				if (!(IDs[0].substring(0, 6).equals(IDs[1].substring(0, 6)))) {
					System.out.println("Warning: prefixes not identical. Calculation probably meaningless.");
				}
			}

		}

	}

	private static void handleBase10Conversion() {
		String base10ToConvertString = commandLine.getOptionValue(CONVERTBASE10SHORT);
		long base10ToConvert = 0;
		try {
			base10ToConvert = Long.parseLong(base10ToConvertString);
		} catch (NumberFormatException e) {
			System.out.println("Tried to convert input value " + base10ToConvertString + " to a number, but failed. Cannot proceed");
			e.printStackTrace();
			System.exit(-1);
		}

		// default pad to 9 characters for SF IDs
		String convertedBase62Id = encodeBase10(base10ToConvert, 9);

		if (isStrict) {
			convertedBase62Id = commandLine.getOptionValue(PREFIXSHORT) + convertedBase62Id;
		}
		System.out.println(convertedBase62Id);
	}

	private static void handleBase62Conversion() {
		String base62ToConvert = commandLine.getOptionValue(CONVERTBASE62SHORT);
		if (base62ToConvert.length() != 18 && base62ToConvert.length() != 15) {
			System.out.println("15 or 18 character Salesforce ID required for conversion form base62 to base 10. Provided: " + base62ToConvert + 
					" (length:" + base62ToConvert.length() + "). Exiting." );
			System.exit(1);
		}

		// we'll throw away the first 6 characters - they're just the object/orgid

		long base10Number = getBase10Number(base62ToConvert.substring(6, 15));

		if (isVerbose) {
			System.out.print("Discarded prefix: " + base62ToConvert.substring(0, 6) + " Base10 number: ");
		}

		System.out.println(base10Number);


	}

	public static long getBase10Number(String base62Id) {

		String idAsString = base62Id;
		if(idAsString.length() > 15) {
			// Drop the case checking suffix for the last 3 characters of an 18 char ID.
			idAsString = idAsString.substring(0, 15);
		}   

		long returnValue = 0;  
		long multiplier = 1;    

		for(int i = idAsString.length(); i > 1; i--) {
			// The character being converted
			String idChar = idAsString.substring(i-1, i);  
			//System.debug(idChar);
			// The index of the character being converted
			long value = base62Chars.indexOf(idChar);

			returnValue = returnValue + ( value * multiplier );  
			multiplier = multiplier * 62;  
		}

		return returnValue;
	}

	public static String generateBase62String(float decimalValue, Integer contractNumberLength) throws Exception
	{
		try
		{
			int outputBase = 62;
			String outputValue = ""; 
			int x;
			base62Chars.length();
			if(decimalValue == 0) {
				return "0";
			}
			else {
				while(decimalValue > 0) {
					x = (int)(((decimalValue/outputBase) - (int)(decimalValue/outputBase))* outputBase + 1.5);
					//X = (Integer)(((DecimalValue/outputBase) - (Integer)(DecimalValue/OutputBase))* OutputBase + 1.5);
					//System.debug('x' + x);
					outputValue = base62Chars.substring(x - 1,x)+outputValue;
					decimalValue = (decimalValue/outputBase);
				}
			}
			//We want to ensure all characters have a value. So if the Base 32 number is 10, and our Contract Number lenght is 5, we want to make the output String "00010"
			while(outputValue.length() < contractNumberLength) {
				outputValue = '0' + outputValue;
			}

			return outputValue.substring(outputValue.length()-9);
		} catch(Exception e) {
			throw new Exception("There was an error converting the base values:" + e.getMessage());
		}
	}

	public static List<String> getExportQueriesForSObject(String objName, String fields, int batchSize, String firstId, String lastId, String gtOrGte) throws Exception {

		List<String> queryList = new ArrayList<String>();
		Integer batchCount = 1;
		long numericFirstId = getBase10Number(firstId.substring(6,15));
		long numericLastId = getBase10Number(lastId.substring(6,15));
		String prefix = ((String)firstId).substring(0,6);
		boolean firstBatch = true;
		for(long recordsProcessed = numericFirstId; recordsProcessed < numericLastId; recordsProcessed += batchSize) {
			String startOperator = ">=";
			long numericEndRange = recordsProcessed + batchSize - 1;
			if (numericEndRange > numericLastId) numericEndRange = numericLastId;
			long numericStartRange = recordsProcessed;
			if (firstBatch) {
				if (gtOrGte != null && gtOrGte.equals("gt")) {
					startOperator = ">";
				}
				firstBatch = false;
			}
			String startRangeId = encodeBase10(numericStartRange, 9);
			String endRangeId = encodeBase10(numericEndRange, 9);
			//	        System.out.println("numericStartRange: " + numericStartRange + " ID:" + startRangeId);
			//    			System.out.println("numericEndRange: " + numericEndRange + " ID:" + endRangeId);
			String soql = /*"BATCH # "+batchCount+ ":*/ "SELECT " + fields + " FROM " + objName + " WHERE Id " + 
					startOperator + " '" + prefix + startRangeId + 
					"' AND Id <= '" + prefix + endRangeId + "' ORDER BY Id asc limit " + batchSize;
			System.out.println(soql);
			queryList.add(soql);

			batchCount++;
		}

		return queryList;
	}

	public static String encodeBase10(long b10, int padding) {
		if (b10 < 0) {
			throw new IllegalArgumentException("b10 must be nonnegative");
		}
		String ret = "";
		while (b10 > 0) {
			ret = base62Chars.charAt((int) (b10 % 62)) + ret;
			b10 /= 62;
		}
		while(ret.length() < padding) {
			ret = '0' + ret;
		}
		return ret;

	}

	/**
	 * Decodes a Base62 <code>String</code> returning a <code>long</code>.
	 * 
	 * @param b62
	 *            the Base62 <code>String</code> to decode.
	 * @return the decoded number as a <code>long</code>.
	 * @throws IllegalArgumentException
	 *             if the given <code>String</code> contains characters not
	 *             specified in the constructor.
	 */
	public static long decodeBase62(String b62) {
		for (char character : b62.toCharArray()) {
			if (!base62Chars.contains(String.valueOf(character))) {
				throw new IllegalArgumentException("Invalid character(s) in string: " + character);
			}
		}
		long ret = 0;
		b62 = new StringBuffer(b62).reverse().toString();
		long count = 1;
		for (char character : b62.toCharArray()) {
			ret += base62Chars.indexOf(character) * count;
			count *= 62;
		}
		return ret;
	}

	private static long decodeSalesforceID (String salesforceID) {
		return decodeBase62(salesforceID.substring(6, 15));
	}
	
	private static void setupOptions() {


		options.addOption( Option.builder("h").longOpt( "help" )
				.desc( "print help message" )
				.build() );



		options.addOption( Option.builder(VERBOSESHORT).longOpt(VERBOSELONG)
				.desc( "spit out more detail in output" )
				.build() );

		OptionGroup operations = new OptionGroup();

		operations.addOption( Option.builder(CONVERTBASE62SHORT).longOpt(CONVERTBASE62LONG)
				.desc( "convert a base62 id to a base10 number" )
				.hasArg().argName("base62id")
				.build() );


		operations.addOption( Option.builder(CONVERTBASE10SHORT).longOpt(CONVERTBASE10LONG)
				.desc( "convert a base10 id to a base62 number" )
				.hasArg().argName("base10number")
				.build() );

		operations.addOption( Option.builder(DISTANCESHORT).longOpt(DISTANCELONG)
				.desc( "calculate a base10 distance between two Salesforce IDs" )
				.hasArg().argName("ID1,ID2")
				.build() );
		operations.addOption( Option.builder(ADDSHORT).longOpt(ADDLONG)
				.desc( "add a base10 number to a base62 ID, get the resulting base62 ID" )
				.hasArg().argName("base62ID,base10NumberToAdd")
				.build() );
		
		operations.addOption( Option.builder(GENERATECHUNKSHORT).longOpt(GENERATECHUNKLONG)
				.desc( "generate queries for chunks of a primary key range. Requires object name (-on), "
						+ "start and end IDs (-si, -ei), and optional query range start (-gt) and field names (fn) parameters.\n"
						+ "If chunksize parameter (-cs) is not provided, will generate 10 chunks for the entire range, as long as the chunks "
						+ "are less than 10M records each, or the required number of 10M record chunks.\n"
						+ "If field names parameter is not provided, will default to the ID field" )
				.build() );
		

		options.addOption( Option.builder(PREFIXSHORT).longOpt(PREFIXLONG)
				.desc( "add a prefix to converted base 10 number" )
				.hasArg().argName("prefix")
				.build() );

		options.addOption( Option.builder(STRICTSHORT).longOpt(STRICTLONG)
				.desc( "strict calculations, i.e. throw error e.g. when prefixes don't match or distance would be negative")
				.build() );
		
		options.addOption( Option.builder(OBJECTNAMESHORT).longOpt(OBJECTNAMELONG)				
				.desc( "Required for -" + GENERATECHUNKSHORT + " operation. Name of the object to generate chunk queries for, "
						+ "e.g. Asset or Product2, etc."  )
				.hasArg().argName("objectname")
				.build() );
		options.addOption( Option.builder(FIELDNAMESHORT).longOpt(FIELDNAMELONG)				
				.desc( "Optional for -" + GENERATECHUNKSHORT + " operation. Commaseparated names of the fields to extract from "
						+ "the object to generate chunk queries for, "
						+ "e.g. Id,Name,LastModifiedBy,My_Custom_Field__c, etc. No spaces please. Will default to just the ID "
						+ "field if not provided."  )
				.hasArg().argName("fieldnamelist")
				.build() );
		options.addOption( Option.builder(STARTIDSHORT).longOpt(STARTIDLONG)				
				.desc( "Required for -" + GENERATECHUNKSHORT + " operation. Starting ID for the range to generate chunk queries for, ")
				.hasArg().argName("SalesforceID")
				.build() );
		options.addOption( Option.builder(ENDIDSHORT).longOpt(ENDIDLONG)				
				.desc( "Required for -" + GENERATECHUNKSHORT + " operation. Ending ID for the range to generate chunk queries for, ")
				.hasArg().argName("SalesforceID")
				.build() );
		options.addOption( Option.builder(OUTPUTFILESHORT).longOpt(OUTPUTFILELONG)				
				.desc( "Optional for -" + GENERATECHUNKSHORT + " operation. Name of the file to generate. "
						+ "Defaults to output.txt if not provided. ")
				.hasArg().argName("filename")
				.build() );
		options.addOption( Option.builder(CHUNKSIZESHORT).longOpt(CHUNKSIZELONG)				
				.desc( "Optional for -" + GENERATECHUNKSHORT + " operation. Size of chunks to generate. "
						+ "Defaults to 10 chunks for less than 100M records or the required number of 10M record chunks if not provided. ")
				.hasArg().argName("chunksize")
				.build() );
		
		options.addOption( Option.builder(RANGESTARTSHORT).longOpt(RANGESTARTLONG)				
				.desc( "Optional for -" + GENERATECHUNKSHORT + " operation. \nIf provided the generated queries will always be "
						+ "ID > startID AND ID =< endID\n"
						+ "If not provided, queries will be ID >= startID AND ID =< endID\\n ")
				.build() );
		options.addOptionGroup(operations);

	}

	private static void printHelp() {
		HelpFormatter formatter = new HelpFormatter();
		formatter.setOptionComparator(null);

		String header = "Do interesting stuff with Salesforce base62 IDs\n\n";
		String footer = "\nRepo at https://github.com/kgalant/PKChunkGenerator";


		formatter.printHelp("java -jar PKChunkGenerator.jar", header, options, footer, true);

		//formatter.printHelp( "java -jar PKChunkGenerator.jar", options );
	}

	private static void parseCommandLine(String[] args) {

		CommandLineParser parser = new DefaultParser();
		try {
			// parse the command line arguments
			commandLine = parser.parse( options, args );
		}
		catch( ParseException exp ) {
			// oops, something went wrong
			System.err.println( "Command line parsing failed.  Reason: " + exp.getMessage() );
			System.exit(-1);
		}
		 if (commandLine.hasOption(VERBOSESHORT)) isVerbose = true;
		 if (commandLine.hasOption(STRICTSHORT)) isStrict = true;
		
	}

	final static String DISTANCESHORT = "d";
	final static String DISTANCELONG = "distance";
	final static String CONVERTBASE62SHORT = "b62";
	final static String CONVERTBASE62LONG = "convertbase62";
	final static String CONVERTBASE10SHORT = "b10";
	final static String CONVERTBASE10LONG = "convertbase10";
	final static String PREFIXSHORT = "pf";
	final static String PREFIXLONG = "prefix";
	final static String VERBOSESHORT = "v";
	final static String VERBOSELONG = "verbose";
	final static String HELPSHORT = "h";
	final static String HELPLONG = "help";
	final static String STRICTSHORT = "st";
	final static String STRICTLONG = "strict";
	final static String ADDSHORT = "a";
	final static String ADDLONG = "add";
	final static String GENERATECHUNKSHORT = "g";
	final static String GENERATECHUNKLONG = "generatechunks";
	final static String OBJECTNAMESHORT = "on";
	final static String OBJECTNAMELONG = "objectname";
	final static String FIELDNAMESHORT = "fn";
	final static String FIELDNAMELONG = "fieldnames";
	final static String STARTIDSHORT = "si";
	final static String STARTIDLONG = "startid";
	final static String ENDIDSHORT = "ei";
	final static String ENDIDLONG = "endid";
	final static String RANGESTARTSHORT = "gt";
	final static String RANGESTARTLONG = "greaterthan";
	final static String OUTPUTFILESHORT = "of";
	final static String OUTPUTFILELONG = "outputfile";
	final static String CHUNKSIZESHORT = "cs";
	final static String CHUNKSIZELONG = "chunksize";
}
