package SkyInterview;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;


/***
 * 
 * @author Terrence
 * This batch job should be called from a job scheduler after a filewatcher job which monitor the arrival of a token file.
 * When the token file comes, the data file of F1 Driver e.g input.csv has been completely sent.
 * 
 *  Usage: java F1DriverTest <input file> <output file>
 *  
 *  e.g.
 *  java F1DriverTest input.csv input_Result.csv 
 *  
 *  If anything was written to System.err, the job should be failed.
 *  
 *  
 *  NOTE: 
 *  This job will fail if the data file is empty in the view that, 
 *  any empty data file should be checked and confirmed before outputting for next process. 
 */

public class F1DriverJob {
	private String strInFilePath;
	private String strOutFilePath;
	
	public static void main(String args[]) {

		if (args.length!=2) {
			System.out.println("Usage: java F1DriverTest <input file> <output file>");
			return;
		}
		F1DriverJob job = new F1DriverJob(args[0], args[1]);
		job.executeJob();
		
	}
	
	public F1DriverJob(String strInFilePath, String strOutFilePath) {
		this.strInFilePath=strInFilePath;
		this.strOutFilePath=strOutFilePath;
	}
	
	public int executeJob() {
		String fileName = strInFilePath;
		String outputFileName = strOutFilePath;
		
		LocalDateTime startTime = LocalDateTime.now();

		System.out.println(startTime.format(DateTimeFormatter.ISO_DATE_TIME) + "  Running: java F1DriverTest " + fileName + " " + outputFileName);
		
		
		//Extraction
		System.out.println(LocalDateTime.now().format(DateTimeFormatter.ISO_DATE_TIME) + "  Extraction begins...");
		
		List<String> records=null;
		try {
			records = Files.readAllLines(Paths.get(fileName));
		} catch (IOException ioe) {
			// failed the job if data file cannot be read
			System.err.println(LocalDateTime.now().format(DateTimeFormatter.ISO_DATE_TIME) + "Error reading file " +fileName+"     Exception message: "+ ioe.getMessage());
			ioe.printStackTrace();
			return -1;
		}
		
		if (records==null || records.isEmpty()) {
			// failed the job if data file is empty
			System.err.println(LocalDateTime.now().format(DateTimeFormatter.ISO_DATE_TIME) + "Empty file was loaded. Failed this job for safety reason.");
			return -1;
		}
		
		Map<String, F1Driver> f1DriverMap = new HashMap<>();
		
		
		// for each record in data file, get the F1Driver from map and add up the lap time
		for (int i=0; i<records.size(); i++) {
			String record=records.get(i);

			String[] nameLapTime=record.split(",");
			if (nameLapTime==null || nameLapTime.length<2) {
				System.err.println(LocalDateTime.now().format(DateTimeFormatter.ISO_DATE_TIME) + " Data file error: No comma separator is  found ("+ record +") on line "+i);
				return -1;
			}
				
			String name=nameLapTime[0];
			if ("".equals(name)) {
				System.err.println(LocalDateTime.now().format(DateTimeFormatter.ISO_DATE_TIME) + " Data file error: No Driver Name on line "+i);
				return -1;
			}
			
			double lapTime =0.0;
			
			try {
				lapTime = Double.parseDouble(nameLapTime[1]);
			} catch (NumberFormatException nfe) {
				System.err.println(LocalDateTime.now().format(DateTimeFormatter.ISO_DATE_TIME) + " Data file error: Invalid number format ("+ nameLapTime[1] +") on line "+i);
				return -1;
			}
				
			F1Driver f1Driver=f1DriverMap.get(name);
			if (f1Driver==null) {
				f1Driver = new F1Driver(name);
				f1DriverMap.put(name, f1Driver);
			}
				
			f1Driver.addTotalLapTime(lapTime);
		}
		
		System.out.println(LocalDateTime.now().format(DateTimeFormatter.ISO_DATE_TIME) + "  Extraction ends...");
		
		
		
		//Transform: lambda call to sort and extract the lowest 3 records. F1Driver.compareTo() was overridden for this.
		System.out.println(LocalDateTime.now().format(DateTimeFormatter.ISO_DATE_TIME) + "  Transform begins...");
		
		List<String> outStrList = f1DriverMap.values().stream().sorted().limit(3).map(s->s.toString()).collect(Collectors.toList());
		System.out.println(LocalDateTime.now().format(DateTimeFormatter.ISO_DATE_TIME) + "  Transform ends...");
		
		
		
		// Load: write the record in file. Handled by F1Driver.toString() 
		System.out.println(LocalDateTime.now().format(DateTimeFormatter.ISO_DATE_TIME) + " Load begins...");
		
		try {
			Files.write(Paths.get(outputFileName), outStrList,
			         StandardOpenOption.CREATE,
			         StandardOpenOption.TRUNCATE_EXISTING );
		} catch (IOException ioe) {
			ioe.printStackTrace();
			System.err.println(LocalDateTime.now().format(DateTimeFormatter.ISO_DATE_TIME) + " Error writing file " +outputFileName+"     Exception message: "+ ioe.getMessage());
			return -1;
		}

		LocalDateTime endTime = LocalDateTime.now();
		System.out.println(LocalDateTime.now().format(DateTimeFormatter.ISO_DATE_TIME) + " Load ends...");
		
		System.out.println(LocalDateTime.now().format(DateTimeFormatter.ISO_DATE_TIME) + " This job completed successfully...");
		
		
		System.out.println("Total Execution time=" + Duration.between(startTime, endTime).toMillis() + "ms");
		return 0;
	}

}

class F1Driver implements Comparable<F1Driver>{
	private String name;
	private	double totalLapTime=0.0;
	private int lapNo=0;
	
	public F1Driver(String name) {
		this.name=name;
	}
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public double getTotalLapTime() {
		return totalLapTime;
	}
	
	public void addTotalLapTime(double totalLapTime) {
		this.totalLapTime += totalLapTime;
		lapNo++;
	}
	
	public double getAvgLapTime() {
		if (lapNo!=0) {
			return totalLapTime/lapNo;
		}
		return 0.0;
	}
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((name == null) ? 0 : name.hashCode());
		return result;
	}
	
	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		
		if (!(obj instanceof F1Driver)) {
			return false;
		}
		
		F1Driver other = (F1Driver) obj;
		if (name == null) {
			if (other.name != null) {
				return false;
			}
		} else if (!name.equals(other.name)) {
			return false;
		}
		return true;
	}
	@Override
	public int compareTo(F1Driver o) {
		return Double.compare(this.getAvgLapTime(), o.getAvgLapTime());
	}
	
	@Override
	public String toString() {
		return name+","+new BigDecimal(getAvgLapTime()).setScale(2, RoundingMode.HALF_UP);
	}
}