/*
 * Created on Oct 7, 2005
 * (c) 2005 Trumpet, Inc.
 *
 */
package jdbm.perftests;

import java.io.IOException;

import jdbm.RecordManager;
import jdbm.helper.Serializer;

/**
 * @author kevin
 */
public class RMDirectBatchInsertPerfTest implements PerfTest{
        RecordManager rm;
        Object obj;
        Serializer serializer;
        int dataSize;

        public RMDirectBatchInsertPerfTest(int dataSize){
            this.dataSize = dataSize;
            serializer = new SimpleSerializer(dataSize);
        }

        public void initialize(RecordManager rm) throws IOException{
            this.rm = rm;
            obj = new Object();
        }
        
        public void doOperation() throws IOException {
            long recid = rm.insert(obj, serializer);
        }
        
        public String toString() {
            return super.toString() + " with data size " + dataSize;
        }
   }