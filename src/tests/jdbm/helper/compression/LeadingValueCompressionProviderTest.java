package jdbm.helper.compression;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Arrays;

import junit.framework.TestCase;

public class LeadingValueCompressionProviderTest extends TestCase {

	LeadingValueCompressionProvider provider;
	protected void setUp() throws Exception {
		provider = new LeadingValueCompressionProvider(0);
	}

	private void doCompressUncompressTestFor(byte[][] groups) throws IOException{
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		DataOutputStream dos = new DataOutputStream(baos);

		ByteArrayCompressor compressor = provider.getCompressor(dos);
		
		for (int i = 0; i < groups.length; i++) {
			compressor.compressNextGroup(groups[i]);
		}
		compressor.finishCompression();
		
		byte[] results = baos.toByteArray();
		
		ByteArrayInputStream bais = new ByteArrayInputStream(results);
		DataInputStream dis = new DataInputStream(bais);
		ByteArrayDecompressor decompressor = provider.getDecompressor(dis);
		
		for(int i = 0;i < groups.length; i++){
			byte[] data_out = decompressor.decompressNextGroup();
			assertTrue(Arrays.equals(groups[i], data_out));
		}
		
	}
	
	private byte[][] getIncrementingGroups(int groupCount, long seed, int lenInit, int comInit, int lenIncr, int comIncr){
		ByteArraySource bap = new ByteArraySource(seed);
		byte[][] groups = new byte[groupCount][];
		for(int i = 0; i < groupCount; i++){
			groups[i] = bap.getBytesWithCommonPrefix(lenInit, comInit);
			lenInit += lenIncr;
			comInit += comIncr;
		}
		return groups;
	}
	
	public void testCompDecompEqualLenEqualCommon() throws IOException{
		byte[][] groups = getIncrementingGroups(
				5, // number of groups 
				1000, // seed
				50, // starting byte array length
				5, // starting common bytes
				0, // length increment
				0 // common bytes increment
				);
		
		doCompressUncompressTestFor(groups);
	}
	
	public void testCompDecompEqualLenIncrCommon() throws IOException{
		byte[][] groups = getIncrementingGroups(
				5, // number of groups 
				1000, // seed
				50, // starting byte array length
				5, // starting common bytes
				0, // length increment
				2 // common bytes increment
				);
		
		doCompressUncompressTestFor(groups);
	}
	
	public void testCompDecompEqualLenDecrCommon() throws IOException{
		byte[][] groups = getIncrementingGroups(
				5, // number of groups 
				1000, // seed
				50, // starting byte array length
				40, // starting common bytes
				0, // length increment
				-2 // common bytes increment
				);
		
		doCompressUncompressTestFor(groups);
	}

	public void testCompDecompIncrLenEqualCommon() throws IOException{
		byte[][] groups = getIncrementingGroups(
				5, // number of groups 
				1000, // seed
				30, // starting byte array length
				25, // starting common bytes
				1, // length increment
				0 // common bytes increment
				);
		
		doCompressUncompressTestFor(groups);
	}
	
	public void testCompDecompDecrLenEqualCommon() throws IOException{
		byte[][] groups = getIncrementingGroups(
				5, // number of groups 
				1000, // seed
				50, // starting byte array length
				25, // starting common bytes
				-1, // length increment
				0 // common bytes increment
				);
		
		doCompressUncompressTestFor(groups);
	}
	
	public void testCompDecompNoCommon() throws IOException{
		byte[][] groups = getIncrementingGroups(
				5, // number of groups 
				1000, // seed
				50, // starting byte array length
				0, // starting common bytes
				-1, // length increment
				0 // common bytes increment
				);
		
		doCompressUncompressTestFor(groups);
	}

	public void testCompDecompNullGroups() throws IOException{
		byte[][] groups = getIncrementingGroups(
				5, // number of groups 
				1000, // seed
				50, // starting byte array length
				25, // starting common bytes
				-1, // length increment
				0 // common bytes increment
				);
		
		groups[2] = null;
		groups[4] = null;
		
		doCompressUncompressTestFor(groups);
	}

}
