package knoelab.classification.output.analysis;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.HashSet;
import java.util.Scanner;
import java.util.Set;

import org.semanticweb.owlapi.apibinding.OWLManager;
import org.semanticweb.owlapi.model.IRI;
import org.semanticweb.owlapi.model.OWLClass;
import org.semanticweb.owlapi.model.OWLDataProperty;
import org.semanticweb.owlapi.model.OWLLogicalAxiom;
import org.semanticweb.owlapi.model.OWLNamedIndividual;
import org.semanticweb.owlapi.model.OWLObjectProperty;
import org.semanticweb.owlapi.model.OWLOntology;
import org.semanticweb.owlapi.model.OWLOntologyManager;

/**
 * This class gathers statistics such as total time taken, total axioms
 * from the output log.
 * 
 * @author Raghava
 *
 */
public class StatsCollector {

	public void collectStatistics(String log) {
		Scanner logScanner = null;
		try {
			long totalLogicalAxioms = 0;
			long totalIndividuals = 0;
			long totalPreProcessingTimeMillis = 0;
			double totalClassificationTimeSecs = 0;        
			Set<OWLLogicalAxiom> logicalAxioms = new HashSet<OWLLogicalAxiom>();
			Set<OWLClass> classes = new HashSet<OWLClass>();
			Set<OWLNamedIndividual> namedIndividuals = new HashSet<OWLNamedIndividual>();
			Set<OWLObjectProperty> objProps = new HashSet<OWLObjectProperty>();
			Set<OWLDataProperty> dataProps = new HashSet<OWLDataProperty>();
	        int numFilesProcessed = -1;
	        boolean print = false;
	        double totalTime1 = 0;
	        double avgFilesWithinTimeLimit = 0;
	        double timeOutValue = 0;
			logScanner = new Scanner(new BufferedReader(
					new FileReader(new File(log))));
			while(logScanner.hasNext()) {
				String line = logScanner.nextLine().trim();
				if(line.isEmpty())
					continue;
				if(line.contains("file")) {
					String[] splitStr = line.split("file:");
					numFilesProcessed = Integer.parseInt(splitStr[0].trim());
					print = false;
				}
				else if(line.startsWith("Logical")) {
					String[] splitStr = line.split(":");
					totalLogicalAxioms += Long.parseLong(splitStr[1].trim());
				}
				else if(line.startsWith("Individuals")) {
					String[] splitStr = line.split(":");
					totalIndividuals += Long.parseLong(splitStr[1].trim());
				}
				else if(line.startsWith("Time")) {
					String[] splitStr = line.split(":");
					totalPreProcessingTimeMillis += 
							Long.parseLong(splitStr[1].trim());
				}
				else if(line.startsWith("real")) {
					print = true;
					String[] splitStr = line.split("\\s");
					String[] minSecs = splitStr[1].split("m");
					long minsInSecs = Long.parseLong(minSecs[0])*60;
					double secs = Double.parseDouble(minSecs[1].split("s")[0]);
					totalClassificationTimeSecs += (minsInSecs + secs);
					
					totalTime1 = ((double)totalPreProcessingTimeMillis/1000) + 
							totalClassificationTimeSecs;
					double avg = totalTime1/(numFilesProcessed+1);
					if(avg <= 20) {
						avgFilesWithinTimeLimit = numFilesProcessed+1;
						timeOutValue = avg;
					}
				}
			}
			System.out.println("Total logical axioms from log: " + totalLogicalAxioms);
			System.out.println("Total logical axioms from owl files: " + logicalAxioms.size());
			System.out.println("Total Individuals from log: " + totalIndividuals);
			System.out.println("Total Individuals from owl files: " + namedIndividuals.size());
//			System.out.println("Total Classes: " + classes.size());
//			System.out.println("Total Object Properties: " + objProps.size());
//			System.out.println("Total Data Properties: " + dataProps.size());
			System.out.println("Total PreProcessing time (secs): " + 
							((double)totalPreProcessingTimeMillis/1000));
			System.out.println("Total Classification time (secs): " + 
							totalClassificationTimeSecs);
			double totalTime2 = ((double)totalPreProcessingTimeMillis/1000) + 
					totalClassificationTimeSecs;
			System.out.println("Total time (secs): " + totalTime2);
			System.out.println("Average time (secs) over 1441 files: " + 
					(totalTime2/1441));
			System.out.println("Average files processed within 20 secs: " + 
					avgFilesWithinTimeLimit + " and time taken: " + timeOutValue);
		}
		catch(Exception e) {
			e.printStackTrace();
		}
		finally {
			if(logScanner != null)
				logScanner.close();
		}
	}

	public static void main(String[] args) {
		if(args.length != 1) {
			System.out.println("Provide output log file");
			System.exit(-1);
		}
		new StatsCollector().collectStatistics(args[0]);
	}
}
