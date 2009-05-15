/**
 * JDBM LICENSE v1.00
 *
 * Redistribution and use of this software and associated documentation
 * ("Software"), with or without modification, are permitted provided
 * that the following conditions are met:
 *
 * 1. Redistributions of source code must retain copyright
 *    statements and notices.  Redistributions must also contain a
 *    copy of this document.
 *
 * 2. Redistributions in binary form must reproduce the
 *    above copyright notice, this list of conditions and the
 *    following disclaimer in the documentation and/or other
 *    materials provided with the distribution.
 *
 * 3. The name "JDBM" must not be used to endorse or promote
 *    products derived from this Software without prior written
 *    permission of Cees de Groot.  For written permission,
 *    please contact cg@cdegroot.com.
 *
 * 4. Products derived from this Software may not be called "JDBM"
 *    nor may "JDBM" appear in their names without prior written
 *    permission of Cees de Groot.
 *
 * 5. Due credit should be given to the JDBM Project
 *    (http://jdbm.sourceforge.net/).
 *
 * THIS SOFTWARE IS PROVIDED BY THE JDBM PROJECT AND CONTRIBUTORS
 * ``AS IS'' AND ANY EXPRESSED OR IMPLIED WARRANTIES, INCLUDING, BUT
 * NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND
 * FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.  IN NO EVENT SHALL
 * CEES DE GROOT OR ANY CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
 * INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
 * SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
 * HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT,
 * STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED
 * OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 * Copyright 2000 (C) Cees de Groot. All Rights Reserved.
 * Copyright 2000-2001 (C) Alex Boisvert. All Rights Reserved.
 * Contributions are Copyright (C) 2000 by their associated contributors.
 *
 * $Id: TestRandomAccessFilePerformance.java,v 1.1 2005/11/08 20:58:27 thompsonbry Exp $
 */

package jdbm.recman;

import java.io.IOException;
import java.io.RandomAccessFile;

import junit.framework.TestCase;

public class TestRandomAccessFilePerformance extends TestCase
{
    public TestRandomAccessFilePerformance() {
        super();
    }
    public TestRandomAccessFilePerformance( String name ) {
        super( name );
    }

    private RandomAccessFile file;
    
    public void setUp()
    	throws IOException
    {
        TestRecordFile.deleteTestFile();        
        file = new RandomAccessFile( TestRecordFile.testFileName + ".db", "rw");
    }

    public void tearDown() throws IOException
    {
        if( file != null ) {
            file.close();        
        }
    }
    
    private final int BLOCK_SIZE = 8196;
    private final byte[] BLOCK = new byte[BLOCK_SIZE];
    
    public void testWrite50M()
    	throws IOException
    {
        int mb = 50;
        int nblocks = ( mb*1024*1024 ) / BLOCK_SIZE; // 50M
        long begin = System.currentTimeMillis();
        for( int i=0; i<nblocks; i++ ) {
            file.write( BLOCK );
        }
        long elapsed = System.currentTimeMillis() - begin;
        System.err.println( "wrote "+mb+"mb ("+nblocks+" blocks) in "+elapsed+"ms");
       
    }
    
}
