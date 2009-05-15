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
 * Contributions are Copyright (C) 2000 by their associated contributors.
 *
 * $Id: ExtensibleSerializer.java,v 1.3 2006/05/29 19:37:08 thompsonbry Exp $
 */
/*
 * Created on Sep 26, 2005
 */
package jdbm.helper;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.HashMap;

import jdbm.RecordManager;
import jdbm.btree.BPage;
import jdbm.btree.BTree;

import org.CognitiveWeb.extser.AbstractExtensibleSerializer;
import org.CognitiveWeb.extser.IExtensibleSerializer;
import org.CognitiveWeb.extser.ISerializer;
import org.CognitiveWeb.extser.LongPacker;

/**
 * Concrete class knows how to maintain its state against a
 * {@link RecordManager}.
 * 
 * @see ExtensibleSerializerSingleton
 * 
 * @author thompsonbry
 */

public class ExtensibleSerializer
   extends AbstractExtensibleSerializer
{

    /**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	/**
     * A reference to the {@link RecordManager}.
     */
    transient private RecordManager m_recman;

    /**
     * The recid of this serializer.
     */
    transient private long m_recid;
    
    /**
     * Return the record manager.
     */
    public RecordManager getRecordManager()
    {
        return m_recman;
    }

    /**
     * Return the logical row id of this serializer.
     */
    public long getRecid()
    {
        return m_recid;
    }

    /**
     * Deserialization constructor.
     */
    
    public ExtensibleSerializer()
    {
        super();
    }
    
    /**
     * Create a new instance an insert it into the store.
     * 
     * @param recman The record manager.
     * @return The new instance.
     * @throws IOException
     */
    public static ExtensibleSerializer createInstance( RecordManager recman )
    	throws IOException
    {
        ExtensibleSerializer ser = new ExtensibleSerializer();
        ser.m_recman = recman;
        ser.m_recid = recman.insert
            ( ser,
              DefaultSerializer.INSTANCE
              );
        ser.registerSerializers(); // register serializers.
        return ser;
    }

    /**
     * Load an existing instance from the store.
     * 
     * @param recman
     * @param recid
     * @return
     * @throws IOException
     */
    
    public static ExtensibleSerializer load( RecordManager recman, long recid )
    	throws IOException
    {
        ExtensibleSerializer ser = (ExtensibleSerializer) recman.fetch
            ( recid,
              DefaultSerializer.INSTANCE
              );
        ser.m_recman = recman;
        ser.m_recid = recid;
        return ser;
    }
    
    /**
     * <p>Note: The {@link DefaultSerializer} is used to insert, fetch
     * and update instances of this class.</p>
     */

    synchronized protected void update()
//    	throws IOException
    {

        try {
        m_recman.update
		( m_recid,
		  this,
		  DefaultSerializer.INSTANCE
		  );
//        System.err.println
//		( "Updated state: #classes="+getClassCount()+", m_recid="+m_recid+", m_recman="+m_recman
//		  );
        }
        catch( IOException ex ) {
            throw new RuntimeException(ex);
        }
        
    }

    public ISerializer getSerializer( long serializerId ) {
        try {
            return (ISerializer) getRecordManager().fetch( serializerId );
        }
        catch( IOException ex ) {
            throw new RuntimeException( ex );
        }
    }

    /**
     * Extends the default behavior to also register serializers for the various
     * jdbm classes with persistent state: {@link BTree}, {@link BPage},
     * etc.
     */
    
    protected void setupSerializers() // throws IOException
    {

        // extend default behavior.
        super.setupSerializers();
        
	// Register serializers for persistent jdbm classes so that we will not have to
	// break binary compatibility if we change any of their serialization formats 
	// in the future.
	_registerClass( jdbm.btree.BTree.class, jdbm.btree.BTree.Serializer0.class, (short) 0 );
	_registerClass( jdbm.btree.BPage.class, jdbm.btree.BPage.Serializer0.class, (short) 0 );
	//_registerClass( jdbm.htree.HTree.class ); (Not serializable.)
	_registerClass( jdbm.htree.HashDirectory.class, jdbm.htree.HashDirectory.Serializer0.class, (short) 0 );
	_registerClass( jdbm.htree.HashBucket.class, jdbm.htree.HashBucket.Serializer0.class, (short) 0 );
	_registerClass( jdbm.strings.StringTable.class, jdbm.strings.StringTable.Serializer0.class, (short) 0 );
	
	// HashMap is used by jdbm for the named object directory, so we pre-register
	// a classId for it now.
	_registerClass( HashMap.class );
        
    }

    public DataOutputStream getDataOutputStream(long recid,
            ByteArrayOutputStream baos) throws IOException {
        return new MyDataOutputStream(recid, this, baos);
    }

    public DataInputStream getDataInputStream(long recid,
            ByteArrayInputStream bais) throws IOException {
        return new MyDataInputStream(recid, this, bais);
    }

    public static class MyDataOutputStream extends DataOutputStream {

        protected MyDataOutputStream(long recid,
                IExtensibleSerializer serializer, ByteArrayOutputStream out)
                throws IOException {
            super(recid, serializer, out);
        }

        public int writePackedOId(long oid) throws IOException {
            return LongPacker.packLong(this, oid);
        }

    }

    public static class MyDataInputStream extends DataInputStream {

        protected MyDataInputStream(long recid,
                IExtensibleSerializer serializer, ByteArrayInputStream is)
                throws IOException {
            super(recid, serializer, is);
        }

        public long readPackedOId() throws IOException {
            return LongPacker.unpackLong(this);
        }

    }

}
