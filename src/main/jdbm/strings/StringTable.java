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
 * $Id: StringTable.java,v 1.3 2006/05/03 19:51:33 thompsonbry Exp $
 */

package jdbm.strings;

import java.io.IOException;
import java.io.Externalizable;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import org.CognitiveWeb.extser.DataInput;
import org.CognitiveWeb.extser.DataOutput;
import org.CognitiveWeb.extser.IStreamSerializer;
import org.CognitiveWeb.extser.Stateless;

import jdbm.btree.BTree;
import jdbm.RecordManager;
import jdbm.helper.IntegerComparator;
import jdbm.helper.IntegerSerializer;
import jdbm.helper.StringComparator;
import jdbm.helper.StringSerializer;

/**
 * A class for interning and disinterning strings.  A single instance
 * of this class is exposed by the {@link BaseRecordManager}.  This
 * implementation uses two btrees to intern and disintern strings. One
 * maps from the string value to a stringId while the other maps from
 * the stringId to the string value.<p>
 * 
 * @author thompsonbry
 */

public class StringTable
    implements Externalizable
{

    /**
     * Transient reference to the {@link RecordManager}. 
     */
    transient RecordManager _recman;
    
    /**
     * Transient logical row id of the {@link StringTable} instance.
     */

    transient long _recid;
    
    /**
     * The recid of the btree that maps from string values to
     * stringIds.
     * 
     * @serial
     */
    
    long m_internId;
    
    /**
     * The recid of the btree that maps from stringIds to string
     * values.
     * 
     * @serial
     */
    long m_disinternId;
    
    /**
     * Cached reference to the BTree used to intern strings.  This
     * maps string values to stringIds.
     */
    
    transient private BTree m_internBTree = null;
    
    /**
     * Cached reference to the BTree used to disintern strings.  This
     * maps stringIds to string values.
     */

    transient private BTree m_disinternBTree = null;

    /**
     * The stringId that always indicates <code>null</code>.
     */

    static public final int NULLID = -1;

    /**
     * The logical row id for the string table instance.
     */
    
    public long getRecid()
    {
        return _recid;
    }
    
    /**
     * Deserialization constructor.
     */
    public StringTable()
    {
    }
    
    /**
     * Create a new {@link StringTable}.
     * 
     * @param recman The record manager.
     * 
     * @return The {@link StringTable}.
     * 
     * @throws IOException
     */
    
    public static StringTable createInstance( RecordManager recman )
    	throws IOException
    {
        StringTable stbl = new StringTable();
        stbl._recman = recman;
        stbl.getInternBTree( recman );
        stbl.getDisinternBTree( recman );
        stbl._recid = recman.insert( stbl );
        return stbl;
    }

    /**
     * Loads an existing {@link StringTable} instance.
     * 
     * @param recman The record manager.
     * 
     * @param recid The logical row id of the instance.
     * 
     * @return The {@link StringTable}.
     * 
     * @throws IOException
     */
    
    public static StringTable load( RecordManager recman, long recid )
    	throws IOException
    {
        StringTable stbl = (StringTable) recman.fetch( recid );
        if( stbl == null ) {
            throw new IllegalArgumentException
               ( "No such record: "+recid
                 );
        }
        stbl._recman = recman;
        stbl._recid = recid;
        return stbl;
    }
    
    /**
     * Conditionally interns <i>s</i> in the string table.  This
     * method may be used to obtain a persistent identifier for
     * commonly used strings.<p>
     * 
     * @param s Some string.
     * 
     * @param insert When <code>true</code> <i>s</i> is interned if is
     * not already in the string table.
     * 
     * @return The stringId associated with <i>s</i>, <code>0</code>
     * iff <code>insert == false</code> and <i>s</i> is not in the
     * string table (zero is never a valid stringId), and
     * <code>-1</code> iff <code>s == null</code>.
     */
    
    synchronized public int intern
	( String s,
	  boolean insert
	  )
	throws IOException
    {

	if( s == null ) {
    
	    return NULLID;
    
	}

	BTree internBTree = getInternBTree( _recman );

	// Lookup existing entry, which is the recid of the
	// interned String.
	
	Integer stringId = (Integer) internBTree.find( s );
	    
	if( stringId == null ) {

	    // This String has not been interned.

	    if( ! insert ) {

		// If not inserting, then we are done.
        
		return 0;

	    }

	    //
	    // Intern a new String.
	    //

	    // Generate the next up stringId. This is based on the #of
	    // stringIds interned so far.  Since we are using [int]
	    // for stringId, it is a runtime error if there are more
	    // stringIds that can fit in an [int].

	    BTree disinternBTree = getDisinternBTree( _recman );

	    long nstrings = disinternBTree.entryCount();
    
	    if( ( nstrings + 1 ) >= Integer.MAX_VALUE ) {
        
		throw new RuntimeException
		    ( "Too many interned strings"
		      );
        
	    }

	    stringId = new Integer( (int) ( nstrings + 1 ) );
    
	    // Insert the value into the disintern tree.  The key is
	    // the one up stringId that we just generated.
    
	    Object oldValue = disinternBTree.insert( stringId, s, false );

	    if( oldValue != null ) {
    
		// paranoia test.
		throw new AssertionError
		    ( "Already used: stringId="+stringId
		      );
        
	    }
    
	    // Insert into the btree: the key is the string, the value
	    // is the stringId we just generated (as an Integer).

	    oldValue = internBTree.insert
	        ( s,
	          stringId,
	          false	// replace (no!)
	          );
		
	    if( oldValue != null ) {
    
		throw new AssertionError
		    ( "Already interned: "+s
		      );
    
	    }

	}

	return stringId.intValue();

    }

    /**
     * Returns the value of an interned string.
     * 
     * @param stringId The stringId of the interned string.
     * 
     * @return The value of the interned string and <code>null</code>
     * iff <i>stringId == -1</i>.
     * 
     * @exception IllegalArgumentException if <i>stringId</i> does not
     * identify an interned string (note that zero (0) never
     * identifies an interned string).
     */
    
    synchronized public String disintern
	( int stringId
	  )
	throws IOException
    {
    
	if( stringId == NULLID ) {
        
	    return null;
        
	}

	BTree disinternBTree = getDisinternBTree( _recman );
        
	String s = (String) disinternBTree.find
	    ( new Integer( stringId )
	      );
        
	if( s == null ) {
            
	    // stringId is not valid.
            
	    throw new IllegalArgumentException
		( "Invalid stringId="+stringId
		  );
            
	}
        
	return s;
        
    }

    /**
     * Returns the btree used to intern strings.  If the btree does
     * not exist then it is created.  If it exsits, then it is loaded.
     * Once in hand it is cached.<p>
     * 
     * The keys are the string values that have been interned.  A
     * compressed key index is used.  The values are the stringIds,
     * represented as an Integer.<p>
     * 
     * @return The btree used to intern strings.
     */

    synchronized private BTree getInternBTree( RecordManager recman )
	throws IOException
    {

	if( m_internBTree == null ) {
	        
	    // Get btree.
	    if( m_internId != 0L ) {
			    
	        // Load btree from store.
	        m_internBTree = BTree.load
		    ( recman,
		      m_internId
		      );
			    
	    } else {

		// Create the btree.  The keys are String values and
		// use leading value compression for Strings.  The
		// values are Integers's.
		    
		m_internBTree = BTree.createInstance
		    ( recman,
		      new StringComparator(),
		      StringSerializer.INSTANCE,
		      IntegerSerializer.INSTANCE
		      );

		// Compressed string keys.
		m_internBTree.setKeyCompressionProvider
		    ( jdbm.helper.compression.LeadingValueCompressionProvider.STRING
		      );

		// Set recid of btree.
		m_internId = m_internBTree.getRecid();

	    }

	}

	return m_internBTree;

    }

    /**
     * Returns the btree used to disintern strings.  If the btree does
     * not exist then it is created.  If it exists, then it is loaded.
     * Once in hand it is cached.<p>
     * 
     * The keys are the stringIds that have been generated.  The
     * values are the string values that were interned.<p>
     * 
     * @return The btree used to disintern strings.
     */

    synchronized private BTree getDisinternBTree( RecordManager recman )
	throws IOException
    {

	if( m_disinternBTree == null ) {
	        
	    // Get btree.

	    if( m_disinternId != 0L ) {
			    
	        // Load btree from store.
	        m_disinternBTree = BTree.load
		    ( recman,
		      m_disinternId
		      );
			    
	    } else {

		// Create the btree.  The keys are Integer values.
		// The values the strings themselves.
		    
		m_disinternBTree = BTree.createInstance
		    ( recman,
		      new IntegerComparator(),
		      IntegerSerializer.INSTANCE,
		      StringSerializer.INSTANCE
		      );

		// Does binary compression help for an [int]?
		//		    m_disinternBTree.setKeyCompressionProvider
		//		        ( jdbm.helper.compression.LeadingValueCompressionProvider.BINARY
		//		          );

		// Save recid of btree.
		m_disinternId = m_disinternBTree.getRecid();
		    
	    }

	}

	return m_disinternBTree;

    }

    //************************************************************
    //********************* Externalizable ***********************
    //************************************************************

    public void writeExternal(ObjectOutput out)
	throws IOException
    {
	out.writeLong( m_internId );
	out.writeLong( m_disinternId );
    }

    public void readExternal(ObjectInput in)
	throws IOException,
	       ClassNotFoundException
    {
	m_internId = in.readLong();
	m_disinternId = in.readLong();
    }

    private static final long serialVersionUID = 5509848259406652128L;

    public static class Serializer0 implements IStreamSerializer, Stateless
    {

        public void serialize(DataOutput out, Object obj) throws IOException {
            StringTable tmp = (StringTable) obj;
            out.writePackedLong( tmp.m_internId );
            out.writePackedLong( tmp.m_disinternId );
        }

        public Object deserialize(DataInput in, Object obj) throws IOException {
            StringTable tmp = (StringTable) obj;
            tmp.m_internId = in.readPackedLong();
            tmp.m_disinternId = in.readPackedLong();
            return tmp;
        }
        
    }
    
}
