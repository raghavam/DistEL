package knoelab.classification.misc;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.PrintWriter;

/**
 * Adds necessary header and footer to the stream of data
 * @author Raghava Mutharaju
 *
 */
public class HeaderFooterAdder {

	public void addHeaderFooter(String inputDirPath, 
			String outputDirPath) throws Exception {
		
		StringBuilder header = new StringBuilder();
		header.append("<?xml version='1.0'?> ").
		append(" <!DOCTYPE rdf:RDF [ ").
		append(" <!ENTITY time 'http://www.w3.org/2006/time#' > ").
		append(" <!ENTITY xsd 'http://www.w3.org/2001/XMLSchema#' > ").
		append(" <!ENTITY rdfs 'http://www.w3.org/2000/01/rdf-schema#' > ").
		append(" <!ENTITY rdf 'http://www.w3.org/1999/02/22-rdf-syntax-ns#' > ").
		append(" <!ENTITY TravelTimeOntology 'http://www.ibm.com/SCTC/ontology/TravelTimeOntology.owl#' > ").
		append(" <!ENTITY CoreSpatioTemporalDataSensorOntology 'http://www.ibm.com/SCTC/ontology/CoreSpatioTemporalDataSensorOntology.owl#' > ").
		append(" <!ENTITY T00 'http://www.ibm.com/SCTC/ontology/TravelTimeOntology.owl#2013-05-07T00:00:00' > ").
		append(" <!ENTITY T21 'http://www.ibm.com/SCTC/ontology/TravelTimeOntology.owl#2012-08-07T21:27:00' > ").
		append(" ]> ").
		append(" <rdf:RDF xmlns='http://www.w3.org/2002/07/owl#' ").
		append(" xml:base='http://www.w3.org/2002/07/owl' ").
		append(" xmlns:rdfs='http://www.w3.org/2000/01/rdf-schema#' ").
		append(" xmlns:time='http://www.w3.org/2006/time#' ").
		append(" xmlns:T00='&TravelTimeOntology;2013-05-07T00:00:00' ").
		append(" xmlns:TravelTimeOntology='http://www.ibm.com/SCTC/ontology/TravelTimeOntology.owl#' ").
		append(" xmlns:xsd='http://www.w3.org/2001/XMLSchema#' ").
		append(" xmlns:rdf='http://www.w3.org/1999/02/22-rdf-syntax-ns#' ").
		append(" xmlns:CoreSpatioTemporalDataSensorOntology='http://www.ibm.com/SCTC/ontology/CoreSpatioTemporalDataSensorOntology.owl#' ").
		append(" xmlns:T21='&TravelTimeOntology;2012-08-07T21:27:00'> ");
//		append(" <Ontology rdf:about='http://www.ibm.com/SCTC/ontology/test1.rdf'/> ");
		   		
		String footer = "</rdf:RDF>";
		
		File streamData = new File(inputDirPath);
		BufferedReader bufferedReader = null;
		PrintWriter printWriter = null;
		try {
			File[] allFiles;
			if(streamData.isDirectory())
				allFiles = streamData.listFiles();
			else {
				// its a file
				allFiles = new File[]{streamData};
			}
			String line;
			StringBuilder optPath = new StringBuilder(outputDirPath + 
					File.separator);
			for(File file : allFiles) {
				String[] fileExt = file.getName().split("\\.");
				bufferedReader = new BufferedReader(new FileReader(file));	
				printWriter = new PrintWriter(new BufferedWriter(
						new FileWriter(new File(optPath.toString() + 
								fileExt[0] + ".owl"))));
				printWriter.println(header.toString());
				while((line = bufferedReader.readLine()) != null) 
					printWriter.println(line);
				printWriter.println(footer);
				
				bufferedReader.close();
				printWriter.close();
			}
		}
		finally {
			if(bufferedReader != null)
				bufferedReader.close();
			if(printWriter != null)
				printWriter.close();
		}
	}
	
	public static void main(String[] args) throws Exception {
		if(args.length != 2) 
			throw new Exception("path to input & output directory");
		new HeaderFooterAdder().addHeaderFooter(args[0], args[1]);
	}

}
