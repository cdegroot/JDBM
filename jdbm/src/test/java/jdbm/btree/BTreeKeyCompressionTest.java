package jdbm.btree;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;

import org.CognitiveWeb.extser.Stateless;

import jdbm.RecordManager;
import jdbm.RecordManagerFactory;
import jdbm.helper.ByteArrayComparator;
import jdbm.helper.ByteArraySerializer;
import jdbm.helper.LongSerializer;
import jdbm.helper.Serializer;
import jdbm.helper.StringComparator;
import jdbm.helper.compression.ByteArraySource;
import jdbm.helper.compression.DefaultCompressionProvider;
import jdbm.helper.compression.LeadingValueCompressionProvider;
import jdbm.recman.TestRecordFile;
import junit.framework.TestCase;

public class BTreeKeyCompressionTest extends TestCase {
	RecordManager recman;
	
	private void reopenRecordManager() throws IOException{
		if (recman != null) recman.close();
		Properties props = new Properties();
		props.setProperty("jdbm.disableTransactions", "true");
		recman = RecordManagerFactory.createRecordManager(TestRecordFile.testFileName);
	}
	
	private void closeRecordManager() throws IOException{
		if (recman != null) recman.close();
		recman = null;
	}
	
	protected void setUp() throws Exception {
		TestRecordFile.deleteTestFile();
		reopenRecordManager();
	}

	protected void tearDown() throws Exception {
		if (recman != null) recman.close();
		TestRecordFile.deleteTestFile();
	}

	private BTree getNewTree(boolean keyCompression) throws IOException{
		BTree tree = BTree.createInstance(recman,  new ByteArrayComparator(), new ByteArraySerializer(), new ByteArraySerializer());
		if (keyCompression)
			tree.setKeyCompressionProvider(new LeadingValueCompressionProvider());
		
		return tree;
	}
	
	private BTree getNewFileSystemBTree(boolean keyCompression) throws IOException{
		BTree tree = BTree.createInstance(recman,  new StringComparator(), new StringSerializer(), new LongSerializer());
		if (keyCompression)
			tree.setKeyCompressionProvider(new LeadingValueCompressionProvider());
		
		return tree;
	}

	/*
	 * Test method for 'jdbm.btree.BTree.setKeyCompressionProvider(ByteArrayGroupCompressionProvider)'
	 */
	public void testSetKeyCompressionProvider() throws Exception {
		BTree tree = getNewTree(false);
		
		tree.setKeyCompressionProvider(new LeadingValueCompressionProvider());
		assertTrue(true);
		tree.insert(new byte[]{0, 1, 2, 3}, new byte[]{0, 1, 2, 3}, true);
		try{
			tree.setKeyCompressionProvider(new DefaultCompressionProvider());
			assertTrue(false);
		} catch (IllegalArgumentException e){}
		
	}

	public void testSimpleInsertion() throws Exception{
		// we are going to insert some keys with common leading bytes
		// then close the recman
		// then reload the tree and make sure we can find each key
		
		BTree tree = getNewTree(true);
		long recid = tree.getRecid();
		
		ByteArraySource provider = new ByteArraySource(42);
		
		int keycount = 500;
		byte[][] testKeys = new byte[keycount][];

		int keyLen = 9;
		int commonLen = 5;
		
		for (int i = 0; i < testKeys.length; i++) {
			testKeys[i] = provider.getBytesWithCommonPrefix(keyLen, commonLen);
			if (i % 7 == 0) keyLen += 3;
			if (i % 13 == 0) commonLen += 2;
			if (i % 11 == 0) keyLen -= 4;
			if (i % 15 == 0) commonLen -= 3;
			if (keyLen < 1) keyLen = (keyLen % 12) + 1;
			if (commonLen < 0 || commonLen > keyLen) commonLen = (commonLen % keyLen) + 1;
		}

		
		for (int i = 0; i < testKeys.length; i++) {
			tree.insert(testKeys[i], testKeys[i], true);	
		}
		
		recman.commit();
		
		for (int i = 0; i < testKeys.length; i++) {
			byte[] val = (byte[])tree.find(testKeys[i]);
			assertTrue(val != null);
			assertTrue(Arrays.equals(testKeys[i], val));
		}
		
		
		tree = null;
		reopenRecordManager();
		
		tree = BTree.load(recman, recid);
		for (int i = 0; i < testKeys.length; i++) {
			byte[] val = (byte[])tree.find(testKeys[i]);
			assertTrue(val != null);
			assertTrue(Arrays.equals(testKeys[i], val));
		}
	}
	
	public void testWithoutCompression() throws Exception{
		doPerformanceTest(false);
	}
	
	public void testWithCompression() throws Exception{
		doPerformanceTest(true);
	}
	
	public void testFileSystemCaptureWithoutCompression() throws IOException{
		doFileSystemCapturePerformanceTest(false);
	}

	public void testFileSystemCaptureWithCompression() throws IOException{
		doFileSystemCapturePerformanceTest(true);
	}
	
	private void doFileSystemCapturePerformanceTest(boolean compress) throws IOException{
		BTree tree = getNewFileSystemBTree(compress);
		
		long start = System.currentTimeMillis();

		File root = new File(".");
		addFilesBelow(tree, root);
		
		closeRecordManager();
		
		long stop = System.currentTimeMillis();
		
		File dbFile = new File(TestRecordFile.testFileName + ".db");
		long fileLen = dbFile.length();

		System.out.println("File System Capture - With" + (compress ? " compression" : "out compression") + " : Added " + tree._entries + " records in " + (stop - start)/1000 + " secs --> " + tree._entries * 1000 / (stop -start) + " inserts / sec  : File size is " + fileLen + " bytes" );
		
	}
	
	private void addFilesBelow(BTree tree, File root) throws IOException{
		File[] children = root.listFiles();
		if (children == null){
			// we probably don't have permissions to read the contents of this directory
			return;
		}
		
		for (int i = 0; i < children.length; i++) {
			File f = children[i];
			tree.insert(f.getCanonicalPath(), new Long(i), true);
			if (f.isDirectory())
				addFilesBelow(tree, f);
		}
	}
	
	public void doPerformanceTest(boolean compress) throws Exception{
		BTree tree = getNewTree(compress);
		
		long start = System.currentTimeMillis();
		
		ByteArraySource provider = new ByteArraySource(42);
		
		int keycount = 5000;

		int keyLen = 9;
		int commonLen = 5;
		byte[] val = {1, 2, 3, 4, 5};
		long totalKeyBytes = 0;
		long totalValBytes = 0;
		
		for (int i = 0; i < keycount; i++) {
			byte[] testKey = provider.getBytesWithCommonPrefix(keyLen, commonLen);
			tree.insert(testKey, val, true);
		
			totalKeyBytes += testKey.length;
			totalValBytes += val.length;
			
			if (i % 7 == 0) keyLen += 3;
			if (i % 13 == 0) commonLen += 2;
			if (i % 11 == 0) keyLen -= 4;
			if (i % 15 == 0) commonLen -= 3;
			if (keyLen < 1) keyLen = (keyLen % 12) + 1;
			if (keyLen > 30) keyLen = (keyLen % 12) + 1;
			if (commonLen > keyLen) commonLen = (commonLen % keyLen) + 1;
			if (commonLen < 5) commonLen = 5;
			
			if (i % 500 == 0) // comment this line out to determine the actual insertion that caused the error - it will be SLOW, though
				try{
					recman.commit();
				} catch (ArrayIndexOutOfBoundsException e){
					System.err.println("Error at commit on insertion " + i);
					throw e;
				}
		}

		closeRecordManager();
		
		long stop = System.currentTimeMillis();
		
		File dbFile = new File(TestRecordFile.testFileName + ".db");
		long fileLen = dbFile.length();
		
		System.out.println("With" + (compress ? " compression" : "out compression") + " : Added " + keycount + " records in " + (stop - start)/1000 + " secs --> " + keycount * 1000 / (stop -start) + " inserts / sec  : File size is " + fileLen + " bytes" );
		System.out.println("Total key bytes: " + totalKeyBytes + ", total val bytes: " + totalValBytes);
	}
	
	static public class StringSerializer implements Serializer, Stateless {
		private static final long serialVersionUID = 1L;

		public StringSerializer() {
			super();
		}

		public byte[] serialize(Object obj) throws IOException {
			String str = (String)obj;
			return str.getBytes();
		}

		public Object deserialize(byte[] serialized) throws IOException {
			return new String(serialized);
		}

	}
}
