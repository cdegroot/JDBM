/*
 * Created on Aug 25, 2005
 * (c) 2005 Trumpet, Inc.
 *
 */
package jdbm.helper;

import junit.framework.TestCase;

/**
 * @author kevin
 */
public class ConversionTest extends TestCase {
    // values for testing packed long representation
    static final long shortBreakVal = Short.MAX_VALUE & 0x7fff;
    static final long intBreakVal = Integer.MAX_VALUE & 0x3fffffff;
    static final long longBreakVal = Long.MAX_VALUE & 0x3fffffffffffffffL;

    public ConversionTest(String arg0) {
        super(arg0);
    }

    private void testVal(long n){
        byte[] buf = Conversion.convertToPackedByteArray(n);
        long rslt = Conversion.recoverLongFromPacked(buf);
        assertTrue(rslt == n);
    }
    
    public void testShouldThrowIllegalArgument(long n) throws Exception{
        try{
            testVal(n);
            assertTrue(false);
        } catch (IllegalArgumentException e){}
    }

    
    private void testShortVal(short val){
        byte[] buf = new byte[2];
        Conversion.pack2(buf, 0, val);
        short valOut;
        valOut = Conversion.unpack2(buf, 0);
        assertTrue(valOut == val);
    }
    /*
     * Test method for 'jdbm.helper.Conversion.unpack2(byte[], int)'
     */
    public void testUnpack2() {
        testShortVal((short)0);
        testShortVal((short)1);
        testShortVal((short)-1);
        testShortVal(Short.MAX_VALUE);
        testShortVal(Short.MIN_VALUE);
    }
    
    public void testAroundShortBoundary() throws Exception{
        testVal(shortBreakVal-2);
        testVal(shortBreakVal-1);
        testVal(shortBreakVal);
        testVal(shortBreakVal+1);
        testVal(shortBreakVal+2);
    }

    public void testAroundIntBoundary() throws Exception{
        testVal(intBreakVal-2);
        testVal(intBreakVal-1);
        testVal(intBreakVal);
        testVal(intBreakVal+1);
        testVal(intBreakVal+2);
    }

    public void testAroundLongBoundary() throws Exception{
        testVal(longBreakVal-3);
        testVal(longBreakVal-2);
        testVal(longBreakVal-1);
        testVal(longBreakVal);
        testShouldThrowIllegalArgument(longBreakVal+1);
    }
    
    public void testForFailures() throws Exception{
        testShouldThrowIllegalArgument(longBreakVal + 1);
        testShouldThrowIllegalArgument(-1);
        testShouldThrowIllegalArgument(Short.MIN_VALUE);
        testShouldThrowIllegalArgument(Integer.MIN_VALUE);
        testShouldThrowIllegalArgument(Long.MIN_VALUE);
            
    }

    /**
     * Test static methods.
     *
     * @todo Rewrite as unit tests?
     */
    public static void main( String[] args )
    {
        byte[] buf;

        buf = Conversion.convertToByteArray( (int) 5 );
        System.out.println( "int value of 5 is: " + Conversion.convertToInt( buf ) );

        buf = Conversion.convertToByteArray( (int) -1 );
        System.out.println( "int value of -1 is: " + Conversion.convertToInt( buf ) );

        buf = Conversion.convertToByteArray( (int) 22111000 );
        System.out.println( "int value of 22111000 is: " + Conversion.convertToInt( buf ) );


        buf = Conversion.convertToByteArray( (long) 5L );
        System.out.println( "long value of 5 is: " + Conversion.convertToLong( buf ) );

        buf = Conversion.convertToByteArray( (long) -1L );
        System.out.println( "long value of -1 is: " + Conversion.convertToLong( buf ) );

        buf = Conversion.convertToByteArray( (long) 1112223334445556667L );
        System.out.println( "long value of 1112223334445556667 is: " + Conversion.convertToLong( buf ) );
    }

}
