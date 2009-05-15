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
 * Copyright 2001 (C) Alex Boisvert. All Rights Reserved.
 * Contributions are Copyright (C) 2001 by their associated contributors.
 *
 * $Id: AbstractRecordCompressor.java,v 1.1 2005/11/08 20:58:28 thompsonbry Exp $
 */
package jdbm.helper.compessor;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.zip.Deflater;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.Inflater;
import java.util.zip.InflaterInputStream;

import jdbm.helper.Serializer;


/**
 * Abstract implementation of a compressor based on the {@link Deflator}and
 * {@link Inflator}utility classes.
 * 
 * @author thompsonbry
 * 
 * FIXME Write test suite.
 */
    
abstract public class AbstractRecordCompressor
    implements IRecordCompressor
{

        public byte[] compress( byte[] bytes )
        {

////            final int level = 9; // compression level in [1:9], not required by inflator.
////            final boolean nowrap = true; // MUST agree with Inflator.
//            
////            Deflater d = new Deflater(level,nowrap);
//            Deflater d = new Deflater();
//            
////            int strategy = Deflater.HUFFMAN_ONLY;
////            d.setStrategy( strategy );
//            
//            byte[] in = toByteArray();
//            
//            d.setInput( in );
//            
//            d.finished();
//            
//            int len = d.getTotalOut();
//            
//            byte[] tmp = new byte[ len ];
//            
//            int tmplen = d.deflate( tmp );
//            
//            if( tmplen != len ) {
//                
//                throw new AssertionError();
//                
//            }
//            
//            return tmp;
        
            ByteArrayOutputStream baos = new ByteArrayOutputStream( bytes.length );
            _deflater.reset();  // required w/ instance reuse.
            DeflaterOutputStream dos = new DeflaterOutputStream( baos, _deflater );
//            DeflaterOutputStream dos = new DeflaterOutputStream( baos );
            
            try {
                dos.write( bytes );
                dos.flush();
                dos.close();
            }
            catch( IOException ex ) {
                throw new RuntimeException( ex );
            }
            byte[] tmp = baos.toByteArray();
            return tmp;

        }

        /**
         * Evidentally a huge portion of the cost associated with the use
         * of the {@link Deflator} is the initialization of a new instance.
         * Also, at least for one application, {@link Deflater#BEST_SPEED}
         * and the default compression algorithm produced equal size stores,
         * so this uses best speed.<p>
         * 
         * Note: This assumes that jdbm serialization is single threaded since
         * there is a single {@link Deflater} instance shared by all requests
         * against this {@link Serializer}.<p>
         */
        
//        private transient Deflater _deflater = new Deflater();
        final private transient Deflater _deflater;

        final private transient Inflater _inflater = new Inflater();

        /**
         * Create a record compressor.
         * 
         * @param level The compression level.
         * 
         * @see Deflater#BEST_SPEED
         * @see Deflater#BEST_COMPRESSION
         */
        protected AbstractRecordCompressor( int level ) {
            _deflater = new Deflater(Deflater.BEST_SPEED);
        }
        
        /**
         * Reused on each invocation and reallocated if buffer size would be
         * exceeded.
         * 
         * @todo This assumes jdbm serialization is single threaded.
         */
        private byte[] _buf = new byte[ 1024 ];
        
        /**
         * Decompresses a byte[] containing the state of the generic object
         * and returned the uncompressed state.
         */
        
        public byte[] decompress( byte[] cmp )
        {

////            final boolean nowrap = true;
////            Inflater i = new Inflater(nowrap);
//            Inflater i = new Inflater();
//            
//            i.setInput( cmp );
//            
//            i.finished();
//            
//            int len = i.getTotalOut();
//            
//            byte[] tmp = new byte[ len ];
//            
//            try {
//                
//                int tmplen = i.inflate( tmp );
//            
//                if( tmplen != len ) {
//                
//                    throw new AssertionError();
//                
//                }
//                
//            }
//            catch( DataFormatException ex ) {
//                
//                throw new RuntimeException( ex );
//                
//            }

            try {

//                InflaterInputStream iis = new InflaterInputStream
//                ( new ByteArrayInputStream( cmp )
//                  );
                
                _inflater.reset(); // reset required by reuse.

                InflaterInputStream iis = new InflaterInputStream
            	( new ByteArrayInputStream( cmp ),
            	  _inflater, cmp.length
            	  );

                int off = 0;
                
                while( true ) { // use bulk I/O.
                
                    int capacity = _buf.length - off;
                    
                    if( capacity == 0 ) {
                        
                        byte[] tmp = new byte[ _buf.length * 2 ];
                        
                        System.arraycopy(_buf,0,tmp,0,off);
                        
                        _buf = tmp;

                        capacity = _buf.length - off;
                        
                    }
                    
                    int nread = iis.read( _buf, off, capacity );
                    
                    if( nread == -1 ) break; // EOF.
                    
                    off += nread;
                    
                }
                
                byte[] tmp = new byte[off];
                
                System.arraycopy(_buf,0,tmp,0,off);
                
                return tmp;
                
            }
            catch( IOException ex ) {
                throw new RuntimeException( ex );
            }
            
        }

}
