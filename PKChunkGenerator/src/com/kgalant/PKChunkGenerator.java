package com.kgalant;

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
			int inputBase = 10;
			int outputBase = 62;
			String outputValue = ""; 
			int x;
			int maxBase = base62Chars.length();
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

	private static void setupOptions() {


		options.addOption( Option.builder("h").longOpt( "help" )
				.desc( "print help message" )
				.build() );



		options.addOption( Option.builder(VERBOSESHORT).longOpt(VERBOSELONG)
				.desc( "spit out more detail in output" )
				.build() );

		OptionGroup operations = new OptionGroup();

		operations.addOption( Option.builder(CONVERTBASE62SHORT).longOpt(CONVERTBASE62LONG)
				.desc( "convert a base62 id to a base10 number <base62id>" )
				.hasArg()
				.build() );


		operations.addOption( Option.builder(CONVERTBASE10SHORT).longOpt(CONVERTBASE10LONG)
				.desc( "convert a base10 id to a base62 number <base10number>" )
				.hasArg()
				.build() );

		operations.addOption( Option.builder(DISTANCESHORT).longOpt(DISTANCELONG)
				.desc( "calculate a base10 distance between two Salesforce IDs <ID1,ID2>" )
				.hasArg()
				.build() );
		operations.addOption( Option.builder(ADDSHORT).longOpt(ADDLONG)
				.desc( "add a base10 number to a base62 ID, get the resulting base62 ID <base62ID,base10NumberToAdd>" )
				.hasArg()
				.build() );

		options.addOption( Option.builder(PREFIXSHORT).longOpt(PREFIXLONG)
				.desc( "add a prefix to converted base 10 number <prefix>" )
				.hasArg()
				.build() );

		options.addOption( Option.builder(STRICTSHORT).longOpt(STRICTLONG)
				.desc( "strict calculations, i.e. throw error e.g. when prefixes don't match or distance would be negative")
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
}
