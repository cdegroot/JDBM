/*
 * Created on Oct 7, 2005
 * (c) 2005 Trumpet, Inc.
 *
 */
package jdbm;

import java.io.IOException;

/**
 * @author kevin
 */
interface PerfTest{
        public void initialize(RecordManager rm) throws IOException;
        public void doOperation() throws IOException;
   }