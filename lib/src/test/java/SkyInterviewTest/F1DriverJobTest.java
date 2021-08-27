package SkyInterviewTest;

import org.junit.jupiter.api.Test;

import SkyInterview.F1DriverJob;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

public class F1DriverJobTest {
    @Test 
    public void testF1DriverJobExexcutionNoInputFile() {
    	System.out.println("Test 1(no input file)");
    	//no input file
    	F1DriverJob classUnderTest = new F1DriverJob("noInputFile.csv", "resultFile1.csv");
        assertTrue(classUnderTest.executeJob()==-1, "executeJob() should return -1");
    }
    
    @Test 
    public void testF1DriverJobExexcutionNoOutputPath() {
    	System.out.println("Test 2 (output path not exists)");
    	//output path not exists
    	F1DriverJob classUnderTest = new F1DriverJob("input.csv", "C:\\abc\\aaa\\resultFile1.csv");
    	assertTrue(classUnderTest.executeJob()==-1, "executeJob() should return -1");
    }
    
    @Test 
    public void testF1DriverJobExexcutionAllSameOne() {
    	System.out.println("Test 3 (All 1)");
    	F1DriverJob classUnderTest = new F1DriverJob("inputAllSameOne.csv", "inputAllSameOne_Result.csv");
    	
    	assertTrue(classUnderTest.executeJob()==0, "executeJob() should return 0");
    	try {
			List<String> listStr = Files.readAllLines(Path.of("inputAllSameOne_Result.csv"));
			for (String currStr:listStr) {
				String[] nameTimePair=currStr.split(",");
				if (Double.parseDouble(nameTimePair[1])!=1) {
					fail("result not equal 1");
				}
			}
		} catch (IOException e) {
			//should not happen as executeJob return 0 which means ended successfully
			fail("no output file.");
		} catch (Exception e) {
			//should not happen as it should return with 3 records with time that can be parsed to double
			fail("data not in correct format.");
		}
    }
    
    @Test 
    public void testF1DriverJobExexcutionEmptyFile() {
    	System.out.println("Test 4 (Empty input file)");
    	//output path not exists
    	F1DriverJob classUnderTest = new F1DriverJob("inputEmpty.csv", "inputEmpty_Result.csv");
    	assertTrue(classUnderTest.executeJob()==-1, "executeJob() should return -1 if input file is empty");
    }
    
    @Test 
    public void testF1DriverJobExexcutionLessThanThreeDrivers() {
    	System.out.println("Test 5 (Less then 3 drivers)");
    	//output path not exists
    	F1DriverJob classUnderTest = new F1DriverJob("inputLessThanThreeDrivers.csv", "inputLessThanThreeDrivers_Result.csv");
    	assertTrue(classUnderTest.executeJob()==0, "executeJob() should return 0");
    	try {
			List<String> listStr = Files.readAllLines(Path.of("inputLessThanThreeDrivers_Result.csv"));
			assertTrue(listStr.size()<3, "result file should be non-empty with less than 3 lines");
		} catch (IOException e) {
			//should not happen as executeJob return 0 which means ended successfully
			fail("no output file.");
		} catch (Exception e) {
			//should not happen as it should return with 3 records with time that can be parsed to double
			fail("data not in correct format.");
		}
    }
    
    @Test 
    public void testF1DriverJobExexcutionInvalidFormat_NoComma () {
    	System.out.println("Test 6 (No Comma)");
    	F1DriverJob classUnderTest = new F1DriverJob("inputNoComma.csv", "inputNoComma_Result.csv");
    	assertTrue(classUnderTest.executeJob()==-1, "executeJob() should return -1");
    	
    }
    
    @Test 
    public void testF1DriverJobExexcutionInvalidFormat_MoreThanOneComma () {
    	System.out.println("Test 7 (More than 1 Comma)");
    	F1DriverJob classUnderTest = new F1DriverJob("inputMoreThanOneComma.csv", "inputMoreThanOneComma_Result.csv");
    	assertTrue(classUnderTest.executeJob()==-1, "executeJob() should return -1");
    }
    
    @Test 
    public void testF1DriverJobExexcutionInvalidFormat_NoTime() {
    	System.out.println("Test 8 (no time)");
    	F1DriverJob classUnderTest = new F1DriverJob("inputNoTime.csv", "inputNoTime_Result.csv");
    	assertTrue(classUnderTest.executeJob()==-1, "executeJob() should return -1");
    }
    
    @Test 
    public void testF1DriverJobExexcutionInvalidFormat_TimeInvalidFormat() {
    	System.out.println("Test 9 (time invalid)");
    	F1DriverJob classUnderTest = new F1DriverJob("inputTimeInvalid.csv", "inputTimeInvalid_Result.csv");
    	assertTrue(classUnderTest.executeJob()==-1, "executeJob() should return -1");
    }
    
    @Test
    public void testF1DriverJobExexcutionInvalidFormat_NoDriverName() {
    	System.out.println("Test 10 (No driver name)");
    	F1DriverJob classUnderTest = new F1DriverJob("inputNoDriverName.csv", "inputNoDriverName_Result.csv");
    	assertTrue(classUnderTest.executeJob()==-1, "executeJob() should return -1");
    }
    
}

