/*
 * Created on Oct 7, 2005
 * (c) 2005 Trumpet, Inc.
 *
 */
package jdbm;

import java.io.IOException;

import jdbm.btree.BTree;
import jdbm.helper.LongComparator;
import jdbm.helper.LongSerializer;
import jdbm.helper.Serializer;

/**
 * @author kevin
 */
public class BTreeBatchInsertPerfTest implements PerfTest{
       BTree bt;
       Object obj;
       long i = 1;
       Serializer serializer;
       int dataSize;
       LongComparator longComparator = new LongComparator();
       LongSerializer longSerializer = new LongSerializer();
       
       public BTreeBatchInsertPerfTest(int dataSize){
           this.dataSize = dataSize;
           serializer = new SimpleSerializer(dataSize);
       }
       
       public void initialize(RecordManager rm) throws IOException{
           this.bt = BTree.createInstance(rm, longComparator, longSerializer, serializer, 26); // 26 ~= 4096 (FSblock size) / 158 (object size + key size)
           obj = new Object();
       }
       
       public void doOperation() throws IOException {
           bt.insert(new Long(i), obj, false);
           i++;
       }
       
       public String toString() {
           return super.toString() + " with data size " + dataSize;
       }
   }