package jdbm;


import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Properties;


/**
* @author kevin
*/
public class PerformanceTestHarness {
   
   
   long startTime = 0;
   long lastDisplayTime = 0;
   long lastOperationsPerformed = 0;
   boolean disableTransactions = true;
   Properties rmProps = null;
   
   private void setRecordManagerProperties(Properties props) {
       this.rmProps = props;
   }

   
   /**
    * returns the path and name of the database file, without the .db extension
    */  
   public String getTestDBFileRoot(){
       return new File(this.getClass().getName() + "_test").getAbsolutePath();
   }
   
   private boolean deleteTestDBFile(){
       File dbFile = new File(getTestDBFileRoot() + ".db");
       File logFile = new File(getTestDBFileRoot() + ".lg");
       if (dbFile.exists() && !dbFile.delete()) return false;
       return logFile.exists() ? logFile.delete() : true;
       
   }
   
   public RecordManager getTestRecordManager(Properties rmProps) throws IOException{
       RecordManager recman = null;
       
//        next, create or open record manager
       recman = RecordManagerFactory.createRecordManager( getTestDBFileRoot() , rmProps );
       
       return recman;
   }
   
   private RecordManager getCleanTestRecordManager() throws IOException{
       if (!deleteTestDBFile()) return null;
       
       return getTestRecordManager(rmProps);
   }
   
   private void runTest(String recmanConfigName, PerfTest test, int opsToPerform, int resultsEvery, boolean disableTransactions, PrintStream out) throws IOException{
       this.disableTransactions = disableTransactions;
       initializeTest(recmanConfigName, test, opsToPerform, out);
       
       RecordManager rm = null;
       
       rm = getCleanTestRecordManager();

       test.initialize(rm);
       
       for(int i = 1; i <= opsToPerform; i++){
           test.doOperation();
           if (i % resultsEvery == 0){
               rm.commit();
               displayResults(i);
           }
           if (!disableTransactions && i % 50 == 0){
               rm.commit();
           }
       }

       rm.close();

       displayResults(opsToPerform);
       out.println("-------------- END OF TEST --------------");
       out.println();

   }
   
   private void initializeTest(String recmanConfigName, PerfTest test, int opsToPerform, PrintStream out){
       out.println("Record manager configuration is " + recmanConfigName);
       out.println("Testing " + test.getClass());
       out.println("Performing " + opsToPerform + " operations");
       out.println("lastDisplayTime, totalOperationsPerformed, elapsedTime, operationsPerSec, OpeartionsPerformedThisTime, elapsedTimeThisTime, operationsPerSecThisTime, ");
       startTime = System.currentTimeMillis();
       lastDisplayTime = startTime;
   }

   private void displayResults(long totalOperationsPerformed){
       long currentDisplayTime = System.currentTimeMillis();
       long elapsedTime = currentDisplayTime - startTime;
       long opsPerSec = elapsedTime > 0 ? totalOperationsPerformed * 1000 / elapsedTime : 0;
       long opsPerformedThisTime = totalOperationsPerformed - lastOperationsPerformed;
       long elapsedTimeThisTime = currentDisplayTime - lastDisplayTime;
       long opsPerSecThisTime = elapsedTimeThisTime > 0 ? opsPerformedThisTime * 1000 / elapsedTimeThisTime : 0;
       
       System.out.println(    lastDisplayTime + ", " +
                           totalOperationsPerformed + ", " +
                           elapsedTime + ", " +
                           opsPerSec + ", " +
                           opsPerformedThisTime + ", " +
                           elapsedTimeThisTime + ", " +
                           opsPerSecThisTime + ", " +
                           ""
                         );
       
       lastDisplayTime = currentDisplayTime;
       lastOperationsPerformed = totalOperationsPerformed;
   }
   
   /**
    * @param args
    * @throws IOException  
    */
   public static void main(String[] args) throws IOException {
       PerformanceTestHarness c =new PerformanceTestHarness();
       
       int operationsToPerform = 10000;
       int resultsEvery = operationsToPerform / 500;

       PerfTest test = new RMDirectBatchInsertPerfTest(4000);
       
       String recmanConfigName = "Tx Disabled";
       Properties props = new Properties();
       props.setProperty("jdbm.disableTransactions", "true");
       
       c.setRecordManagerProperties(props);
       c.runTest(recmanConfigName, test, operationsToPerform, resultsEvery, true, System.out);
   }

   
}
