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
 * $Id: DumpUtility.java,v 1.3 2005/11/08 20:58:28 thompsonbry Exp $
 */

package jdbm.recman;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.io.DataInputStream;
//import java.io.ObjectInputStream;
//import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
//import java.io.ByteArrayOutputStream;
//import java.io.ObjectOutputStream;

import java.util.Arrays;
import java.util.Map;
import java.util.TreeMap;
import java.util.Iterator;
import java.util.Properties;
import java.util.Comparator;
import java.util.Vector;

import jdbm.helper.DefaultSerializer;
import jdbm.helper.ExtensibleSerializer;
import jdbm.helper.ISerializationHandler;
import jdbm.helper.Serializer;
import jdbm.helper.compression.CompressionProvider;

import jdbm.RecordManager;
import jdbm.RecordManagerOptions;
import jdbm.RecordManagerFactory;

import jdbm.btree.BPage;
import jdbm.btree.BTree;
//import jdbm.helper.Tuple;
//import jdbm.helper.TupleBrowser;

/**
 * A utilty for dumping some aspects of the file structure of jdbm, especially
 * the different page lists.
 * <p>
 * 
 * Note: {@link BPage}instances will not show up unless you are using the
 * extensible serialization handler.
 * <p>
 * 
 * @author thompsonbry
 * 
 * @todo Stat the file for the total size of the store and write percentages on
 *       the major histograms (this class has NNN instances and is XX% of the
 *       store). For accurate measurements, we need to account for the record
 *       headers (or measure percentage of the total record size).
 * 
 * @todo For some reason the keys (and values) of BTrees are not being scanned
 *       for strings. Perhaps I am using compressed key indices whenever there
 *       is string keys, which would mean that the serialized record did not
 *       have much string data?
 */

public class DumpUtility
    extends BaseRecordManager
{

    private int pageMarkerCount = 10;
    
    /**
     * A marker is written on the console every N pages as an
     * inidicator of progress.
     */
    public int getPageMarkerCount() {return pageMarkerCount;}

    public void setPageMarkerCount( int value )
    {
        
        pageMarkerCount = value;
        
    }
    
    private int _pageLimit = 0;
    
    /**
     * When non-zero, specifies a limit on the #of pages on any
     * given list that will be scanned.
     */
    public int getPageLimit() {return _pageLimit;}
    
    public void setPageLimit( int value )
    {
        
        _pageLimit = value;
        
    }
    
    /**
     * Helper method used to ensure that {@link DumpUtility} is not
     * instantiated for a store file which does not exist.
     */

    static private String exists( String filename )
    {

        if( ! new File( filename+".db" ).exists() ) {
            
            throw new IllegalArgumentException
                ( "Could not find file: "+filename
                  );
                
        }

	return filename;
        
    }

    /**
     * Opens the named jdbm file in a read-only mode and provides a
     * variety of utility methods for examining the state of the
     * various jdbm page lists, including the data pages.<p>
     */
    
    public DumpUtility( String filename )
    	throws IOException
    {

	super( exists( filename ) );

	// Turn off transactions.  They are not necessary for a read-only
	// store.
	
        _file.disableTransactions();
        
    }
    
    private IRecordAnalyzer _recordAnalyzer = new DefaultRecordAnalyzer();
    
    public void setRecordAnalyzer( IRecordAnalyzer recordAnalyzer ) {
        if( recordAnalyzer == null ) {
            throw new IllegalArgumentException();
        }
        _recordAnalyzer = recordAnalyzer;
    }
    
    public IRecordAnalyzer getRecordAnalyzer() {
        return _recordAnalyzer;
    }
    
    /**
     * @exception IOException since the store is read-only.
     */

    public synchronized void delete( long recid )
    	throws IOException
    {
        
        throw new IOException( "read-only" );
        
    }
    
    /**
     * @exception IOException since the store is read-only.
     */

    public synchronized void update( long recid, Object obj, Serializer serializer )
    	throws IOException
    {
            
            throw new IOException( "read-only" );

    }
    
    private int _binDecls[] = defaultBinDecls;
    
    /**
     * An array of integers sorted into an ascending order which defines
     * the size of the bins used to collect histograms based on record
     * size or capacity.  The default value is {@link #defaultBinDecls}.<p>
     * 
     * Note: jdbm records may be up to 2^31 bytes long since a signed
     * integer is used to record the length of the record.<p>
     */
    
    public int[] getHistogramBins()
    {
        
        return _binDecls;
        
    }

    /**
     * Sets the histogram bins to be applied.
     * 
     * @param bins An array of bin sizes in sorted order.
     * 
     * @return The old histogram bins.
     */

    public int[] setHistogramBins( int[] bins )
    {
     
        if( bins == null ) {
            
            throw new IllegalArgumentException();
            
        }
        
        // Make sure that the bins are sorted.
        Arrays.sort( bins );

        // Old bins - return to caller.
        int[] ret = _binDecls;
        
        _binDecls = bins;
        
        return ret;
        
    }
    
    /**
     * Returns an index into <i>bins</i> of the first entry greater than or
     * equal to <i>n</i>.<p>
     * 
     * @param bins An array of integers in an ascending sort which defines
     * the bins into which frequency counts will be aggregated. 
     * 
     * @param n The size or capacity of some record.  In jdbm the maximum
     * size or capacity of a record is 2^31.
     * 
     * @return The index into the array of bins for <i>n</i>
     */
    
    static protected int getBin( int[] bins, int n )
    {

        for( int i=0; i<bins.length; i++ ) {
            
            if( bins[ i ] >= n ) return i;
            
        }
        
        throw new AssertionError
           ( "Could not find bin: n="+n
             );
        
    }
    
    /**
     * Writes out the non-zero entries from a histogram.
     * 
     * @param ps
     * @param label The label.
     * @param bins The histogram data.
     * @param _binDecls The bins used to collect those data.
     */

    static protected void writeHistogram( PrintStream ps, String label, long[] bins, int[] binDecls )
    {

        // Percentages equal to or greather than this threshold are reported.
        // This threshold is set above zero so that very small buckets will be
        // ignored.  This makes the fine-grained histograms much more useful.
        final int threshold = 1;
        
        long sum = 0L;
        
        for( int i=0; i<bins.length; i++ ) {
            
            sum += bins[ i ];
            
        }
        
        ps.println( label+": #samples="+sum );
        
        ps.println( "count\tpercent\tbinSize" );
        
        for( int i=0; i<bins.length; i++ ) {
            
            if( bins[ i ] == 0 ) continue;
            
            int percent = ((int)(((double)bins[i]/(double)sum)*100d*100d))/100;
            
            if( percent >= threshold ) {
            
                ps.println( bins[ i ]+"\t"+percent+"%\t"+binDecls[ i ] );
                
            }
            
        }
        
    }

    /**
     * Write out the per record type statistics in order by the most frequently occurring
     * record type.  For each record type, histograms by record size and record capacity
     * are also written.
     */
    
    public void writeHistogram( PrintStream ps, String label, Map typedata )
    	throws IOException
    {

        // Percentages equal to or greather than this threshold are reported.
        // The per-record-type threshold is set to zero so that all record
        // types will be reported.
        final int threshold = 0;
        
        //
        // Insert the data into a private map ordered by the #of records of
        // a given type.  We also count the #of samples in the histogram as
        // we go.
        //
        
        long nsamples = 0L;
        
        Map order = new java.util.TreeMap();
        
        Iterator itr = typedata.values().iterator();
        
        while( itr.hasNext() ) {
            
            RecordTypeData tdata = (RecordTypeData) itr.next();
            
            long count = tdata.getCount();
            
            nsamples += count;
            
            order.put( new Long( count ),
                       tdata
                       );
            
        }

        //
        // Write out the histogram.
        //
        
        ps.println( label+": #samples="+nsamples );
        
        ps.println( "count\tpercent\ttype" );        

        itr = order.values().iterator();
        
        while( itr.hasNext() ) {
            
            RecordTypeData tdata = (RecordTypeData) itr.next();

            String type = tdata.getType();
            
            long count = tdata.getCount();
            
            int percent = ((int)(((double)count/(double)nsamples)*100d*100d))/100;
            
            if( percent >= threshold ) {
                
                ps.println( count+"\t"+percent+"%\t"+type  );
                
            }
            
        }

        //
        // Now visit all the record types again and this time write out the record
        // size histogram for each record type.
        //
        
        itr = order.values().iterator();
        
        while( itr.hasNext() ) {
            
            RecordTypeData tdata = (RecordTypeData) itr.next();

            String type = tdata.getType();

            tdata.writeSizeHistogram( ps, "\nRecord size histogram: "+type );
            
        }
        
    }
    
    /**
     * Writes a summary of the free page list.
     * 
     * @param ps
     * 
     * @throws IOException
     */
    
    public void writeFreePageListSummary( PrintStream ps )
    	throws IOException
    {
        
        checkIfClosed();
        
        ps.println( "\n*** Free Page List Summary." );

        long npages = 0L;
        
        // Setup a cursor over the free page list.
        
        PageCursor curs = new PageCursor
            ( _pageman,
              Magic.FREE_PAGE
              );
        
        final int pageLimit = getPageLimit();
        
        while( curs.next() != 0 ) {

            if( pageLimit != 0 && npages >= pageLimit ) {
                
                // Stop early.
                
                break;
                
            }
            
	    if( npages > 0L && npages % pageMarkerCount == 0 ) {

		System.err.print( "." );
		
	    }

	    long blockId = curs.getCurrent();
            
            //ps.print( "Scanning free page="+blockId );
            
            try {
                
            BlockIo block = _file.get( blockId );

            PageHeader view = PageHeader.getView
                ( block
                  );
           
            npages++; // #of pages scanned.

            }
            
            finally {
                
                // Release the page.
                
                _file.release( curs.getCurrent(), false );
                
            }
            
        }
    
        ps.println( "\nScanned "+npages+" page(s) on the free page list." );
                
    }
    
    /**
     * Scans the free logical id page list and reports on what it
     * finds there.  Each page contains a number of "slots."  Slots
     * are either available or unavailable.  If a slot is available,
     * then it contains a logical row id, which is an index into the
     * translation table.  Such logical row ids can be reallocated,
     * in which case the same recid can refer to distinct records in
     * the store over the life cycle of the application (which could
     * be a problem if you are holding onto such recids externally).
     *  
     * @param ps
     * 
     * @throws IOException
     */
    
    public void writeFreeLogicalRowIdPageListSummary( PrintStream ps )
    	throws IOException
    {
    
        checkIfClosed();

        ps.println( "\n*** Free Logical Row Id Page List Summary." );

        // #of pages on the free logical row id page list.
        long npages = 0L;
        
        final int slotsPerPage = FreeLogicalRowIdPage.ELEMS_PER_PAGE;
        
        ps.println( "#slots per page="+slotsPerPage );

        // Setup a cursor over the free logical ids page list.
        
        PageCursor curs = new PageCursor
            ( _pageman,
              Magic.FREELOGIDS_PAGE
              );
        
        final int pageLimit = getPageLimit();
        
        while( curs.next() != 0 ) {

            if( pageLimit != 0 && npages >= pageLimit ) {
                
                // Stop early.
                
                System.err.println( "\n*** Stopping after: "+npages+" pages." );
                
                break;
                
            }
            
	    if( npages > 0L && npages % pageMarkerCount == 0 ) {

		System.err.print( "." );
		
	    }

	    long blockId = curs.getCurrent();
            
            ps.print( "Scanning free logical row id page="+blockId );
            
            try {
                
            BlockIo block = _file.get( blockId );

            FreeLogicalRowIdPage view = FreeLogicalRowIdPage.getFreeLogicalRowIdPageView
                ( block
                  );
           
            int nfreeThisPage = view.getCount();
            
            ps.println( " #of unused logical row ids (this page)="+nfreeThisPage );

            for( int slot=0; slot<slotsPerPage; slot++ ) {

                if( view.isFree( slot ) ) continue; // skip slots that are not allocated.
                
                PhysicalRowId tmp = view.get( slot );
                
                ps.println( "Slot="+slot+" has unused logical row id="+new Location( tmp ) );
                
            }
            
            npages++; // #of translation pages scanned.

            }
            
            finally {
                
                // Release the page since we have finished scanning it.
                
                _file.release( curs.getCurrent(), false );
                
            }
            
        }
    
        ps.println( "\nScanned "+npages+" page(s) on the free logical row id page list." );
        
    }
    
    /**
     * Scans the free physical id page list and reports on what it
     * finds there.  Each page contains a number of "slots."  Slots
     * are either available or unavailable.  If a slot is available,
     * then it contains the physical row id (block and offset) of an
     * available physical row (a deleted record).  Such rows can be
     * reallocated.
     * 
     * @param ps
     * 
     * @throws IOException
     */
    
    public void writeFreePhysicalRowIdPageListSummary( PrintStream ps )
    	throws IOException
    {
    
        checkIfClosed();

        ps.println( "\n*** Free Physical Row Id Page List Summary." );

        // #of pages on the free physical row id page list.
        long npages = 0L;
        
        // #of bytes of free capacity across all deleted records.
        long freecapacity = 0L;

        // #of deleted records.
        long nfreerecs = 0L;

        // The total #of "free" records aggregated into bins by record
        // capacity.
        long freecapacitybin[] = new long[ getHistogramBins().length ];

        final int slotsPerPage = FreePhysicalRowIdPage.ELEMS_PER_PAGE;
        
        ps.println( "#slots per page="+slotsPerPage );

        // Setup a cursor over the free physical row ids page list.
        
        PageCursor curs = new PageCursor
            ( _pageman,
              Magic.FREEPHYSIDS_PAGE
              );
        
        final int pageLimit = getPageLimit();
        
        while( curs.next() != 0 ) {

            if( pageLimit != 0 && npages >= pageLimit ) {
                
                // Stop early.
                
                System.err.println( "\n*** Stopping after: "+npages+" pages." );
                
                break;
                
            }
            
	    if( npages > 0L && npages % pageMarkerCount == 0 ) {

		System.err.print( "." );
		
	    }

	    long blockId = curs.getCurrent();
            
            ps.print( "Scanning free physical row id page="+blockId );
            
            try {
                
            BlockIo block = _file.get( blockId );

            FreePhysicalRowIdPage view = FreePhysicalRowIdPage.getFreePhysicalRowIdPageView
                ( block
                  );
           
            int nfreeThisPage = view.getCount();
            
            ps.println( " #of free physical rows (this page)="+nfreeThisPage );

            for( int slot=0; slot<slotsPerPage; slot++ ) {

                if( view.isFree( slot ) ) continue; // skip free slots.
                
                FreePhysicalRowId tmp = view.get( slot );
                
                int capacity = tmp.getSize();
                
                ps.println( "Slot="+slot+" has free physical row "+new Location( tmp )+" with capacity="+capacity );

                freecapacity += capacity;
                
                freecapacitybin[ getBin( getHistogramBins(), capacity ) ]++;
                
                nfreerecs++;
                
            }
            
            npages++; // #of translation pages scanned.

            }
            
            finally {
                
                // Release the page since we have finished scanning it.
                
                _file.release( curs.getCurrent(), false );
                
            }
            
        }
    
        ps.println( "\nScanned "+npages+" page(s) on the free physical row id page list." );

        ps.println( "There are "+nfreerecs+" deleted records in the store." );
        
        ps.println( "Total capacity of deleted records is "+freecapacity+" bytes." );
        
        writeHistogram( ps, "\nFree capacity histogram", freecapacitybin, getHistogramBins() );
        
    }

    /**
     * A slot on one of the page types.
     * 
     * @author thompsonbry
     */
    public interface Slot
    {
    }
    
    /**
     * Interface scans all slots on a page and constructs an array
     * containing their data.  This wraps up the get/release on the
     * page so that operations on the slots defined on that page may
     * then perform recman operations without concern that they could
     * trigger a "double get on block ..." exception.
     * 
     * @author thompsonbry
     */
    
    public static interface PageScanner
    {
    }
    
    /**
     * The data identifying a slot in a translation page (the logical
     * row id) and the data found in that slot (the physical row id).
     * 
     * @author thompsonbry
     */
    public static class TranslationPageSlot implements Slot
    {
        
        private long _recid;
        private Location _physid;
        
        /**
         * Return the logical row id for the record in this slot.
         */
        
        public long getRecid() {return _recid;}
        
        /**
         * Return the physical row id for the record in this slot.
         * This may be used to fetch the record header.
         */

        public Location getPhysicalRowId() {return _physid;}
        
        /**
         * 
         * @param recid The logical row id for the record.
         * @param physid The physical row id for the recod.
         */

        public TranslationPageSlot( long recid, Location physid )
        {
            _recid = recid;
            _physid = physid;
        }
        
    }

    /**
     * Gets a translation page, extracts the in use slots, and then
     * releases the translation page.
     * 
     * @author thompsonbry
     */
    
    public static class TranslationPageScanner
    	implements PageScanner
    {

        final RecordFile _file;
        
        public TranslationPageScanner( BaseRecordManager recman )
        {
            _file = recman._file;
        }

        /**
         * Scans a translation page and returns an array of the logical recids
         * found on that page.
         * 
         * @param blockId The translation page to be scanned.
         * 
         * @return The logical row ids found on that page.
         * 
         * @throws IOException
         */
        
        public Slot[] scanPage( long blockId )
            throws IOException
        {

            Vector slots = new Vector();

            try {
                
		BlockIo transBlock = _file.get( blockId );
		
		TranslationPage xlatPage = TranslationPage.getTranslationPageView
		    ( transBlock
		      );
		
		for( int i=0; i<TranslationPage.ELEMS_PER_PAGE; i++ ) {
		    
		    // Compute the offset to the ith entry on this
		    // translation page.  Normally the blockId for the
		    // translation page is taken from the block
		    // component of the Location and the offset into
		    // the translation page is taken from the offset
		    // component of the Location.  Since we are just
		    // scanning the translation page list we get the
		    // block from the page list and have to compute
		    // the offsets ourselves.
                
		    short offset = (short) ( TranslationPage.O_TRANS + ( i * PhysicalRowId.SIZE ) );
		    
		    // The logical row id for the record that we are considering.

		    long recid = new Location( blockId, offset ).toLong();
		    
		    // Note: Slots in the translations page are cleared 
		    // when a record is deleted.  The slot is also zero
		    // if that logical record has never been allocated.
		    // Either way, we skip these slots.
                
		    Location physid = new Location( xlatPage.get( offset ) );
		    
		    if( physid.getBlock() == 0 ) {
			
			// Skip empty slot.
			
			// ps.println( "Skipping: "+physid );
			
			continue;
			
		    }
		    
		    TranslationPageSlot slot = new TranslationPageSlot
		    	( recid,
		    	  physid
		    	  );
		    
		    slots.add( slot );

		} // next slot on translation page.
            
            } // try
        
            finally {
            
                // Release the translation page since we have finished
                // scanning the records linked from that page.
            
                _file.release( blockId, false );
            
            }
        
            return (TranslationPageSlot[]) slots.toArray
                ( new TranslationPageSlot[]{}
                  );

        }
        
    }

    /**
     * Helper class collects statistics related to the allocation of
     * the {@link BTree} in the store.
     * 
     * @author thompsonbry
     * 
     * @todo collect histogram of node sizes and write out with the
     * btree summary.
     */
    
    public static class BTreeStatistics
    {
       
        final private long m_btreeId;
        
        private long m_totalNodeSize = 0L;
        private long m_nodeCount = 0L;
        
        public BTreeStatistics( long btreeId ) {
            m_btreeId = btreeId;
        }
        
        public long getBTreeId() {
            return m_btreeId;
        }

        /**
	 * Return the total size of the records comprising the nodes
	 * of the {@link BTree}.
	 */

        public long getTotalNodeSize() {
            return m_totalNodeSize;
        }
        
        /**
         * Return the #of {@link BPage} nodes in the {@link BTree}.
         */

        public long getNodeCount() {
            return m_nodeCount;
        }
        
	/**
	 * Collect statistics from a {@link BPage} owned by the {@link
	 * BTree}.
	 *
	 * @param recid The recid of the {@link BPage}.
	 *
	 * @param size The size of the record in which the {@link
	 * BPage} was stored.
	 *
	 * @param bpage The deserialized {@link BPage}.
	 */

        public void collect( long recid, int size, BPage bpage )
	{

	    if( bpage.getBTree().getRecid() != getBTreeId() ) {

		throw new AssertionError();

	    }

            m_totalNodeSize += size;

            m_nodeCount++;
            
        }
        
    }
    
    /**
     * Scans the list of translation pages and summarizes what it
     * finds there.  The translation table is a linked list of pages.
     * Each page has an array of entries.  Each entry maps a logical
     * row id (a persistent identifier for a record) into a physical
     * row id (an block and offset in the file).  A given logical row
     * id may be remapped onto a different physical row id during the
     * life cycle of the record -- typically this happens when the
     * record is updated and its new size exceeds its capacity.<p>
     * 
     * Deleted records do not appear in the translation table.  Data
     * on deleted records is summarized by {@link
     * #writeUsedPageListSummary( PrintStream ps )} and detailed by
     * {@link #writeFreePhysicalRowIdPageListSummary( PrintStream ps
     * )}.<p>
     * 
     * The data which can be obtained by scanning the translation
     * table is mostly redundent with simply scanning the {@link
     * Magic#USED_PAGE} list -- see {@link #writeUsedPageListSummary(
     * PrintStream ps )} which does the latter.<p>
     *
     * Data records are scanned for strings.  A frequency count of
     * such strings is accumulated by the <i>scanner</i>.  A report of
     * that frequency count is written as well.  This is useful for
     * identifying strings which could be interned in the store and
     * for identifying unoptimized object serialization (Java
     * serialization writes the class name into the object output
     * stream).<p>
     * 
     * Instances of {@link BTree} are noted as we go and summarized
     * separately.<p>
     *
     * @param scanner When non-null, the scanner is used to scan for
     * strings in the data.
     * 
     * @see #writeUsedPageListSummary( PrintStream ps )
     * 
     * @author thompsonbry
     */
    
    public void writeTranslationPageListSummary
	( PrintStream ps,
	  StringScanner scanner
	  )
    	throws IOException
    {
        
        checkIfClosed();
        
        ps.println( "\n*** Translation Page List Summary." );
        
        // #of translation pages scanned.
        long npages = 0L;
        
//        // #of records scanned.
//        long nrecs = 0L;

        // #of "in use" records (non-zero size).
        long nuserecs = 0L;
        
//        // #of "free" records (zero size, but non-zero capacity).
//        long nfreerecs = 0L;
        
        // total of "size" across all in use records.
        long usesize = 0L;
        
        // #of in use records in which there is some waste.
        long nwasterecs = 0L;
        
        // #of bytes of waste across all in use records.
        long totalwaste = 0L;
        
        // #of bytes capacity across all in use records.
        long usecapacity = 0L;

//        // #of bytes capacity of currently deleted records.
//        long freecapacity = 0L;
        
        // #of bytes capacity of all records with any wasted capacity.
        long wastecapacity = 0L;
        
        // The total #of "in use" records aggregated into bins by record
        // size.
        long usesizebin[] = new long[ getHistogramBins().length ];

//        // The total #of "free" records aggregated into bins by record
//        // capacity.
//        long freecapacitybin[] = new long[ _binDecls.length ];
        
        // Map from record type to statistics for that record type.
        Map typedata = new java.util.TreeMap();

        /* Keys are Long(recid) of each BTree; values are the total serialized
         * size of the bpages owned by that BTree.  Note that the BPage record
         * size information is only available when using the extensible serialization
         * handler.
         */
        Map btrees = new java.util.HashMap();
        
        //
        // Setup a cursor over the translation page list.
        //
        
        PageCursor tcurs = new PageCursor
            ( _pageman,
              Magic.TRANSLATION_PAGE
              );
        
        final int pageLimit = getPageLimit();
        
        while( tcurs.next() != 0 ) {

            if( pageLimit != 0 && npages >= pageLimit ) {
                
                // Stop early.
                
                System.err.println( "\n*** Stopping after: "+npages+" pages." );
                
                break;
                
            }
            
	    if( npages > 0L && npages % pageMarkerCount == 0 ) {

		System.err.print( "." );
		
	    }

	    final long blockId = tcurs.getCurrent();
            
//            ps.println( "Scanning translation page="+transBlockId );

            // Extract slots from the translation page.
            final TranslationPageSlot[] slots = (TranslationPageSlot[]) new TranslationPageScanner
            	( this ).scanPage
            	( blockId
            	  );

            // Scan records for each logical row id found on the translation page. 
            for( int i=0; i<slots.length; i++ ) {

                // The logical row id.
                long recid = slots[ i ].getRecid();

                Location physid = slots[ i ].getPhysicalRowId();

                /* Size of the record (in bytes).
                 */
                int size;
                
                /*
                 * The #of pages across which the record was written. This is
                 * one (1) most of the time, but a record that crosses a page
                 * boundary will result in a value larger that one.
                 * 
                 * @todo Collect this metric. It probably means scanning the
                 * blocks to get it right. An estimate would see if the record
                 * crosses the first page boundary (based on the offset at which
                 * it starts in the page) and the divide through the remainder
                 * of the record size by the space available on an empty page
                 * for a record to get the #of pages spanned.
                 */
                int pagesSpanned;
                
		    // ps.println( "   Examining: "+physid );
		    
		    //
		    // Fetch the record header, which is on the first
		    // page for the record.  The page is identified by
		    // the block component of [physid].  The offset to
		    // the record on that page is given by the offset
		    // component of [physid].
		    //
		    
		    try {
			
			BlockIo block2 = _file.get( physid.getBlock() );
			
			RecordHeader head = new RecordHeader
			    ( block2,
			      physid.getOffset()
			      );
			
			// ps.println( head.toString() );
			
			size = head.getCurrentSize();
			
			int capacity = head.getAvailableSize();
			
			if( size <= 0 ) {

			    throw new AssertionError
			       ( "Not expecting deleted record to be listed in translation page list"+
			         ": physid="+physid+
			         ", header="+head
			         );

			    // If the size is zero, then the record
			    // was "deleted".
			    
//			    freecapacity += capacity;
//			    
//			    nfreerecs++;
			    
			    // ps.println( head+" : free." );
			    
//			    freecapacitybin[ getBin( _binDecls, capacity ) ]++;
			    
			} else {
			    
			    nuserecs++;
			    
			    usesize += size;
			    
			    usecapacity += capacity;
			    
			    usesizebin[ getBin( getHistogramBins(), capacity ) ]++;
			    
			    int waste = capacity - size;
			    
			    if( waste > 0 ) {
				
				// ps.println( head+" : waste="+waste+" bytes." );
				
				totalwaste += waste;
				
				wastecapacity += capacity;
				
				nwasterecs++;
				
			    }
			    
			}
			
//			nrecs++; // #of records scanned (free + in use).
			
		    } // try
		    
		    finally {
			
			// release the record.
			
			_file.release( physid.getBlock(), false );
			
			
		    }
		    
		    // Aggregate histogram data by record type.

		    String className = collectRecordTypeData
		            ( recid,
		              physid,
		              typedata,
		              scanner
		              );
		        
		    if( BTree.class.getName().equals( className ) ) {
		                
		        // If we have not already seen a BPage for this BTree,
		        // then allocate an object for the statistics for the
		        // BPages in this BTree and add it into the map under
		        // the recid for the BTree.
		        
		        if( ! btrees.containsKey( new Long( recid ))) {
		            
		            btrees.put( new Long( recid ), new BTreeStatistics( recid ) );
		        
		        }
		                
		    }

		    if( BPage.class.getName().equals( className ) ) {

		        // Try to fetch the BPage.  This will only succeed if you
		        // are using the extensible serialization handler since it
		        // is otherwise not possible to deserialize a BPage outside
		        // of the context of the owning BTree.
		        
		        try {

		            BPage bpage = (BPage) fetch( recid );
		            
		            long btreeId = bpage.getBTree().getRecid();
		            
		            BTreeStatistics stats = (BTreeStatistics) btrees.get( new Long( btreeId ));
		            
		            if( stats == null ) {

		                // This happens if we see the BPage before we see the BTree.
		                
		                stats = new BTreeStatistics( btreeId );

		                btrees.put( new Long( btreeId ), stats );

			    }
		            
		            // Collect data on this BPage.
		            stats.collect( recid, size, bpage );
		            
		        }
		        catch( IOException ex ) {
		            // Ignore - we could not fetch/deserialize the BPage.
		        }
		        
		    }

            } // next slot on translation page.
                
            npages++;
            
        } // next page using cursor.
	
//        // #of bytes capacity across all records (in use and free).
//        long totalcapacity = usecapacity + freecapacity;
        
        ps.println( "\nScanned "+npages+" pages on the 'translation' page list." );
        
        ps.println( "There are "+nuserecs+" in use records with a capacity of "+usecapacity+" bytes." );

        ps.println( "Total waste of "+totalwaste+" bytes across "+nwasterecs+" records in which waste occurs." );
        
        ps.println( "Wasted capacity for in use records: "+ ((int)(((double)totalwaste/(double)usecapacity)*100d*100d))/100.+"%" );
        
        ps.println( "Average waste per record with waste: "+(totalwaste/nwasterecs)+" bytes." );

        ps.println( "Average size of record with waste: "+(wastecapacity/nwasterecs)+" bytes." );
        
        ps.println( "Waste per record with waste: "+((int)(((double)totalwaste/(double)wastecapacity)*100d*100d))/100.+"%");
        
        ps.println( "Average in use record size: "+(usesize/nuserecs) );

        writeHistogram( ps, "\nIn use size histogram ", usesizebin, getHistogramBins() );

//        writeHistogram( ps, "\nFree capacity histogram", freecapacitybin );
        
        writeHistogram( ps, "\nRecord type histogram", typedata );

        ps.println( "\nBTrees: #btree="+btrees.size() );

        Iterator itr = btrees.entrySet().iterator();
        
        while( itr.hasNext() ) {
            
            Map.Entry entry = (Map.Entry) itr.next();

            long recid = ((Long)entry.getKey()).longValue();
            
            BTreeStatistics stats = (BTreeStatistics) entry.getValue();
        
            writeBTreeSummary( ps, recid, stats );
            
        }
     
        if( scanner != null ) {
            
            ps.println( "\nString scanner summary" );
            
            scanner.writeReport( ps );
            
        }
        
    }

    /**
     * Writes a summary of the btree.
     * 
     * @param ps
     *            Where to write the summary.
     * 
     * @param recid
     *            The recid of the {@link BTree}.
     * 
     * @param stats
     *            A helper object used to collect statistics about the nodes in
     *            the btree.
     * 
     * @throws IOException
     */
    
    public void writeBTreeSummary( PrintStream ps, long recid, BTreeStatistics stats )
    	throws IOException
    {

        BTree btree;
        
        try {

            //
            // Load the btree instance from the store.
            //
            
            btree = BTree.load( this, recid );
            
        }
        
        catch( Throwable t ) {
            
            if( t instanceof jdbm.helper.WrappedRuntimeException ) {
            
                if( ((jdbm.helper.WrappedRuntimeException)t).getException() instanceof ClassNotFoundException ) {
                    
                    // This can happen if the BTree uses application
                    // specific serializers, keys, or value objects
                    // which are not on the classpath.
                
                    System.err.println
                    ( "Check CLASSPATH - could not fetch BTree: "+t
                      );
                  
                    return;
                    
                }
                
            }
                
            // Rethrow the exception.
                
            throw new RuntimeException( t );
            
        }

        //
        // Write overview of btree state.
        //
        
        ps.println( "\nBTree: recid="+recid+", pageSize="+btree.getPageSize()+", height="+btree.getHeight()+", entryCount="+btree.entryCount() );
        
        Comparator keyComparator = btree.getComparator();
        CompressionProvider keyCompressionProvider = btree.getKeyCompressionProvider();
        Serializer keySerializer = btree.getKeySerializer();
        Serializer valueSerializer = btree.getValueSerializer();

        ps.println( "keyComparator: "+(keyComparator!=null?keyComparator.getClass():null) );
        ps.println( "keySerializer: "+(keySerializer!=null?keySerializer.getClass():null) );
        ps.println( "valueSerializer: "+(valueSerializer!=null?valueSerializer.getClass():null) );
        ps.println( "keyCompressionProvider: "+(keyCompressionProvider!=null?keyCompressionProvider.getClass():null) );

        // total record size for btree nodes.
        long totalBytes = stats.getTotalNodeSize();
        long totalMB    = stats.getTotalNodeSize() / (1024 * 1024);
        long nodeCount  = stats.getNodeCount();
        
        ps.println( "#of nodes in btree: "+nodeCount);
        ps.println( "Total record size for nodes in btree: "+totalBytes+"(bytes) "+totalMB+"(mb)" );
        ps.println( "Average node size in btree: "+totalBytes/nodeCount+"(bytes)");
        
    }
    
    /**
     * Maintains count of the #of records of a certain "type."
     */
    static class RecordTypeData
    {
        
        /** The record type name or null if using classId. */
        final private String m_type;
        /** The bin declarations for the histogram. */
        final private int[] m_binDecls;
        /** The #of records of this type / classId. */
        private long m_count = 0L;

        /**
         * The count of the #of records of this type aggregated into bins
         * by record size.
         */
        final private long sizebin[];

        /**
         * The count of the #of records of this type aggregated into bins
         * by record capacity.
         */
        final private long capbin[];

        /**
         * Constructor.
         * 
         * @param type The name of the record type.
         *  
         * @param _binDecls Declares the bins to be used by the per record type
         * histograms.
         */

        public RecordTypeData( String type, int[] binDecls )
        {
            
            m_type = type;
            
            m_binDecls = binDecls;

            sizebin = new long[ binDecls.length ];
            
            capbin = new long[ binDecls.length ];
            
        }
        
        /**
         * Count one more record of this type and builds up both a size and capacity
         * histogram for this record type.
         * 
         * @param size The size of the record.
         * 
         * @param capacity The capacity of the record.
         */
        
        public void inc( int size, int capacity )
        {
         
            m_count++;

	    sizebin[ DumpUtility.getBin( m_binDecls, size ) ]++;

	    capbin[ DumpUtility.getBin( m_binDecls, capacity ) ]++;

        }

        /**
         * The record type.
         */
        public String getType() {return m_type;}

        public long getCount() {return m_count;}

        /**
         * Writes out the histogram of record size for records of this type.
         */
        
        public void writeSizeHistogram( PrintStream ps, String label )
        {
            
            DumpUtility.writeHistogram
               ( ps, label, sizebin, m_binDecls
                 );
            
        }

        /**
         * Writes out the histogram of record capacity for records of this type.
         */

        public void writeCapacityHistogram( PrintStream ps, String label )
        {
            
            DumpUtility.writeHistogram
               ( ps, label, capbin, m_binDecls
                 );
            
        }
        
    }

    /**
     * Fetch the record and attempts to categorize it as a member of some class
     * (record type) and builds up a distribution of the size of records of that
     * type.
     * 
     * @param typedata
     *            Used to collect histograms per record type.
     * 
     * @param scanner
     *            Used to scan records for strings (optional).
     * 
     * @return The class name if it was recognized and otherwise
     *         <code>null</code>.
     */

    protected String collectRecordTypeData
	( long recid,
	  Location physid,
	  Map typedata,
	  StringScanner scanner
	  )
    	throws IOException
    {

        // Fetch the record.  We don't use the recman API here since
        // we don't know which Serializer to apply to deserialize the
        // record, but we need to type it anyway and scan it for strings.
        byte[] data = _physMgr.fetch( physid );
        try {
            data = _compressor.decompress( data );
        }
        catch( RuntimeException ex ) {
            // This message will be reported for any records that are
            // written w/o compression since the will lack a compress
            // header.  It can be safely ignored.
            System.out.println
                ( "WARN: Could not decompress record: recid="+recid+", ex="+ex
                  );
        }

	if( scanner != null ) {

	    // Scan for strings in the serialized record.
	    scanner.scan( data );

	}
        
        String type = getRecordAnalyzer().getRecordType( this, recid, data );

        if( type == null || type.length() == 0 ) {
            
            throw new AssertionError( "Record type must be non-null and non-empty label.");
            
        }
        
        RecordTypeData tdata = (RecordTypeData) typedata.get
           ( type
             );
        
        if( tdata == null ) {
            
            tdata = new RecordTypeData( type, getHistogramBins() );
            
            typedata.put( type, tdata );
            
        }

        int size = data.length;
        
        int capacity = 0; // @todo The capacity is only coded on the record header.
        
        tdata.inc( size, capacity );

        // Figure out if the "type" is actually a class name.
        try {
            Class.forName( type );
            return type; // this is the name of some class.
        }
        catch( ClassNotFoundException ex ) {
            return null; // not the name of any class that we recognize.
        }

    }

    /**
     * Summarizes all data records in the store whether they contain
     * valid data not.  Each jdbm record has both a "size" and a
     * "capacity".  This if the size is zero, then the record has been
     * deleted.  Otherwise if the capacity is greater than the "size",
     * then there is some wasted space in that record.<p>
     *
     * Here's the strategy:<ul>
     *
     * <li> Get the first data page and create a data page view.  This
     * tells us the offset of the first record on the page.
     * 
     * <li> Create a record header view for the first record on the
     * page.
     * 
     * <li> The consumed space (size) from the record header tells you
     * how much of that record is actually used for real data.  The
     * available size (capacity) tells you how much waste there is in
     * the record.  If the size is zero, then the record is
     * deleted.
     * 
     * <li> Query the record header view for the size of the record
     * (available size, not bytes actually used).  Add that many bytes
     * to the current offset in the block (plus the size of the
     * header), and repeat until all records on the page have been
     * exhausted.
     * 
     * <li> When you drop off the end of a page, move to the next page
     * (as determined by the data page view), determine the correct
     * offset in that page to start with, and repeat.
     * 
     * </ul>
     * 
     * @param ps Where to write the data.
     * 
     * @todo This is not collecting a histogram by record type since
     * we already have the block and that would constitute a
     * double-get for jdbm if we tried to use the existing API to
     * fetch the record data.  You can see the record type histogram
     * using {@link #writeTranslationPageListSummary( PrintStream ps,
     * StringScanner scanner )}. [this can be fixed by writing a method
     * that scans all the slots on the page and then releases the page
     * while returning the slots.  we can then process the slots and 
     * fetch the record if we please.]
     */

    public void writeUsedPageListSummary(PrintStream ps)
    	throws IOException
    {
        
        checkIfClosed();
        
        ps.println( "\n*** Used Page List Summary" );
        
        // #of translation pages scanned.
        long npages = 0L;
        
        // #of records scanned.
        long nrecs = 0L;

        // #of "in use" records (non-zero size).
        long nuserecs = 0L;
        
        // #of "free" records (zero size, but non-zero capacity).
        long nfreerecs = 0L;
        
        // total of "size" across all in use records.
        long usesize = 0L;
        
        // #of in use records in which there is some waste.
        long nwasterecs = 0L;
        
        // #of bytes of waste across all in use records.
        long totalwaste = 0L;
        
        // #of bytes capacity across all in use records.
        long usecapacity = 0L;

        // #of bytes capacity of currently deleted records.
        long freecapacity = 0L;
        
        // #of bytes capacity of all records with any wasted capacity.
        long wastecapacity = 0L;
        
        // The total #of "in use" records aggregated into bins by record
        // size.
        long usesizebin[] = new long[ getHistogramBins().length ];

        // The total #of "free" records aggregated into bins by record
        // capacity.
        long freecapacitybin[] = new long[ getHistogramBins().length ];
        
//        // Map from record type to statistics for that record type.
//        Map typedata = new java.util.TreeMap();
        
        //
        // Setup a cursor over the used page list.
        //
        
        PageCursor tcurs = new PageCursor
            ( _pageman,
              Magic.USED_PAGE
              );
        
        while( tcurs.next() != 0 ) {

            long usedBlockId = tcurs.getCurrent();
            
//            ps.println( "Scanning used page="+usedBlockId );
           
            try {
                
		BlockIo usedBlock = _file.get( usedBlockId );
		
		DataPage dataPageView = DataPage.getDataPageView
		    ( usedBlock
		      );
		
		// offset to the first record header on this data page.
		short offset = dataPageView.getFirst();
		
		if( offset == 0 ) {
		    
		    // No record starts on this page.  This can happen
		    // if the data page is entirely (other than its
		    // header) devoted to a continuation of a record
		    // which begins on another page.
		    
		    continue;
		    
		}
		
		while( true ) {
		    
		    //
		    // Get record header.
		    //
		    
		    RecordHeader head = new RecordHeader( usedBlock, offset );
		    
		    //
		    // Collect statistics on record.
		    //
		    
//		    ps.println( head.toString() );
		    
		    int size = head.getCurrentSize();
		    
		    int capacity = head.getAvailableSize();
		    
		    if( capacity == 0 ) {
		        
		        // Note: This is the only indication that we have when
		        // there are no more records allocated on a page.  If 
		        // the rest of the page is NOT filled with zeros, then
		        // there is a chance that the dump utility will attempt
		        // to create record header views when there is no such
		        // record.
		        
		        break;
		        
		    }
		    
		    if( size <= 0 ) {
			
			// If the size is zero, then the record was
			// "deleted".
			
			freecapacity += capacity;
			
			nfreerecs++;
			
//			ps.println( head+" : free." );
			
			freecapacitybin[ getBin( _binDecls, capacity ) ]++;
			
		    } else {
			
			nuserecs++;
                    
			usesize += size;
			
			usecapacity += capacity;
			
			usesizebin[ getBin( _binDecls, capacity ) ]++;
			
			int waste = capacity - size;
			
			if( waste > 0 ) {
			    
//			    ps.println( head+" : waste="+waste+" bytes." );
			    
			    totalwaste += waste;
			    
			    wastecapacity += capacity;
			    
			    nwasterecs++;
			    
			}
			
		    }
		    
		    nrecs++; // #of records scanned (free + in use).
		    
		    //
		    // Compute offset of the next record header on
		    // this page.
		    //
                
		    offset += head.getAvailableSize() + RecordHeader.SIZE;
		    
		    if( offset >= ( RecordFile.BLOCK_SIZE - RecordHeader.SIZE ) ) {
			
			// Fall off the end of this data page.
			
			break;
			
		    }
		    
		} // next record on this page.
		
		// Fall through - get the next data page using the
		// page cursor.
		
		npages++; // #of used pages scanned.

            } // try
            
            finally {
                
                // Release the data page since we have finished
                // scanning the records linked from that page.
                
                _file.release( tcurs.getCurrent(), false );
                
            }
            
        } // page cursor loop.

        // #of bytes capacity across all records (in use and free).
        long totalcapacity = usecapacity + freecapacity;
        
        ps.println( "\nScanned "+npages+" pages on the 'used' page list." );
        
        ps.println( "Records: used="+nuserecs+", free="+nfreerecs+", total="+nrecs );
        
        ps.println( "Capacity: used="+usecapacity+", free="+freecapacity+", total="+totalcapacity );

        ps.println( "% free capacity: "+ ((int)(((double)freecapacity/(double)totalcapacity)*100d*100d))/100. );

        ps.println( "Total waste of "+totalwaste+" bytes across "+nwasterecs+" records in which waste occurs." );
        
        ps.println( "% wasted capacity: "+ ((int)(((double)totalwaste/(double)totalcapacity)*100d*100d))/100. );
        
        ps.println( "Average waste per record with waste: "+(totalwaste/nwasterecs)+" bytes." );

        ps.println( "Average size of record with waste: "+(wastecapacity/nwasterecs)+" bytes." );
        
        ps.println( "% waste per record with waste: "+((double)totalwaste/(double)wastecapacity)*100);
        
        ps.println( "Average in use record size: "+(usesize/nuserecs) );

        writeHistogram( ps, "\nIn use size histogram ", usesizebin, getHistogramBins() );

        writeHistogram( ps, "\nFree capacity histogram", freecapacitybin, getHistogramBins() );
        
//        writeHistogram( ps, "\nRecord type histogram", typedata );
        
    }

    //************************************************************
    //************************************************************
    //************************************************************

    /**
     * Scans binary data for strings and builds up a frequency
     * count.<p>
     *
     * Note: This assumes that a byte corresponds to one character.<p>
     */

    public static class StringScanner
    {
	
	/**
	 * When true, the string scanner uses a case-sensitive
	 * comparison.
	 */
	
	final private boolean m_caseSensitive;
	
	/**
	 * Minimum string length to be counted.
	 */
	
	final private int m_minLength;
	
	/**
	 * Maximum string length - excess is truncated.  All strings
	 * whose leading prefix up to this threshold compare as equals
	 * will be counted as equals.
	 */
	
	final private int m_maxLength;
	
	/**
	 * At least this many instances to be reported.  Since the
	 * recman supports interning strings, there should be a
	 * threashold of two (2) for most uses of this string scanner.
	 */

	final private int m_minReport;
	
	/**
	 * Map from String to frequency count (wrapped as Long).
	 */

	final private Map freqCount;

	/**
	 * #of string instances observed (which pass our criteria).
	 */
	private long ninstances = 0L;

	/**
	 * #of distinct strings observed (which pass our criteria).
	 */
	public long getStringCount() {return freqCount.size();}

	/**
	 * #of string instances observed (which pass our criteria).
	 */
	public long getInstanceCount() {return ninstances;}

	/**
	 * Defaults to case sensitive with a minimum string length of
	 * 5, a maximum string length of 80, and a minimum reported
	 * frequency count of 2.
	 */
	public StringScanner()
	{

	    this( true, 5, 80, 2 );
	    
	}

	/**
	 * @param caseSensitive When false all strings are forced to
	 * the same case.
	 * @param minLength The minimum length of a string that will
	 * be collected.
	 * @param maxLength The maximum length of a string that will
	 * be collected.  Strings longer than this are still scanned,
	 * but they are truncated such that all strings longer than
	 * this threshold with the same <i>maxLength</i> leading
	 * characters will be considered to be the same string value.
	 * @param minReport The minimum frequency count to report.
	 */

	public StringScanner
	    ( boolean caseSensitive,
	      int minLength,
	      int maxLength,
	      int minReport
	      )
	{

	    m_caseSensitive = caseSensitive;
	    
	    m_minLength = minLength;

	    m_maxLength = maxLength;

	    m_minReport = minReport;
	    
	    freqCount = new TreeMap();

	}

	/**
	 * Writes a report of the identified strings ordered by
	 * inverse frequency count.
	 */

	public void writeReport( PrintStream ps )
	{

	    ps.println( "minLength="+m_minLength);
	    ps.println( "maxLength="+m_maxLength);
	    ps.println( "minReport="+m_minReport);
	    ps.println( "caseSensitive="+m_caseSensitive);
	    ps.println( "Strings: #distinct="+getStringCount()+", #instances="+getInstanceCount());

	    // Build up map ordered by inverse frequency count.  We
	    // drop out strings that do not meet the minimum frequency
	    // count here.

	    Map ordered = new TreeMap( new ReverseLongComparator() );

	    Iterator itr = freqCount.entrySet().iterator();

	    while( itr.hasNext() ) {

		Map.Entry entry = (Map.Entry) itr.next();

		String key = (String) entry.getKey();

		Long value = (Long) entry.getValue();

		if( value.longValue() >= m_minReport ) {

		    ordered.put( value, key );

		}

	    }

	    // Write out the data.

	    itr = ordered.entrySet().iterator();

	    ps.println( "freq\tkb\tstring" );

	    while( itr.hasNext() ) {

		Map.Entry entry = (Map.Entry) itr.next();

		Long freq = (Long) entry.getKey();

		String str = (String) entry.getValue();

		long kb = ( freq.longValue() * str.length() ) / 1024;
		
		ps.println( ""+freq+"\t"+kb+"\t"+str );

	    }	    

	}

	/**
	 * Scans <i>data</i> looking for strings, accumulating a
	 * frequency count of distinct strings.  This is useful when
	 * examining a store for unoptimized serialization of classes
	 * and strings which could be interned.<p>
	 *
	 * @param data The serialized form of some record.
	 */
    
	public void scan( final byte[] data )
	{
	    
	    final int len = data.length;
	    
	    int i = 0; // index into data[].
	    int j = 0; // index into chars[] (of next char to write,
	    	       // i.e., the #of characters in chars[]).
	    
	    // buffer to accumulate characters.
	    char[] chars = new char[len];
	    
	    while( true ) {

	        if( i >= len ) {
	            
	            if( j > 0 ) {
	                
	                report( j, chars );
	                
	            }
	            
	            return;
	            
	        }
	        
	        char ch = (char) data[ i ];
	        
	        if( ch>=32 && ch<127 ) {
	            
	            chars[ j++ ] = ch;
	            
	        } else if( j > 0 ) {
	            
	            report( j, chars );
	            
	            j = 0; 
	            
	        }

		i++;		// next byte/char.
	        
	    }
	    
	}

	/**
	 * Reports an array of <i>n</i> characters in <i>chars</i> at
	 * offset zero.  If the string meets the criteria, then it is
	 * cumulated in the frequency count.
	 * 
	 * @param n #of valid characters in <i>chars</i>
	 * @param chars Array of characters forming the string.
	 */
	
	private void report( int n, final char[] chars )
	{
	    
	    if( n >= m_minLength ) {
	        
	        // Respect maximum string length.
	        n = Math.min( n, m_maxLength );
	        
	        // Use only the first [n] characters.
	        String s = new String( chars, 0, n );
	        
	        if( ! m_caseSensitive ) {
	            
	            // force upper case.
	            
	            s.toUpperCase();
	            
	        }
	        
	        Long oldCount = (Long) freqCount.get( s );
	        
	        if( oldCount == null ) {
	            
	            freqCount.put( s, ONE );
	            
	        } else {
	            
	            Long newCount = new Long
	            	( oldCount.longValue() + 1
	            	  );
	            
	            freqCount.put( s, newCount );
	            
	        }

		ninstances++;
	        
	    }
	    
	}

	/**
	 * The {@link Long} whose value is ONE (1).
	 */

	static private final Long ONE = new Long(1L);
	
	/**
	 * Used to sort the data by inverse frequency count.
	 */
	
	private static class ReverseLongComparator
		implements Comparator
        {

	    public int compare(Object o1, Object o2)
	    {
	        return - ((Long)o1).compareTo((Long)o2);
	    }
		
        }
	
    }

    //************************************************************    
    //************************************************************    
    //************************************************************    

    /**
     * Writes a usage message on stderr and exits with a non-zero
     * status code.
     * 
     * @param args The command line arguments.
     */
    
    public static final void usage( String msg, String[] args )
    {
        
        System.err.println( msg );
    
        System.err.println
           ( "DumpUtility [options] filename\n"+
             "   -A\tAll (equivilent to -F -T -U -L -P).\n"+
             "   -F\tSummarize the free page list.\n"+
             "   -T\tSummarize the translation page list (includes record type breakdown).\n"+
             "   -U\tSummarize the used page list (includes deleted record summary).\n"+
             "   -L\tSummarize the free logical row id page list.\n"+
             "   -P\tSummarize the free physical row id page list.\n"+
             
             "   -fg\tUse fine grained histograms.\n"+
             
             "   -ss\tScan for strings in data.\n"+
             "   -ssi\tCase insensitive string scan (default is case sensitive).\n"+
             "   -ssml#\tMinimum length for scanned strings to # (default is 5).\n"+
             "   -ssML#\tMaximum length for scanned strings to # (default is 80).\n"+
             "   -ssmf#\tMinimum frequency count to report for scanned strings to # (default is 2).\n"+
             
             "   -pm#\tSets the page marker count to #.\n"+
             "   -pl#\tSets the page limit count to #.\n"+
             
             "   -ra{cl} Names the {@link IRecordAnalyzer} implementation class.\n"+
             
             "\nThe default behavior is equivilent to (-U)."
             );

        System.exit( 1 );
        
    }

    /**
     * The histogram bin declarations that are used by default.  These
     * bins are based on powers of 2 {2, 4, 8, etc.} up to 2^16 and
     * then there is one more bin at 2^31, which is the largest record
     * size permitted by jdbm.
     */
    
    public static final int[] defaultBinDecls = new int[] {
    2, 4, 8, 16, 32, 64, 128, 256,
    512, 
    1024,
    2048,
    4096,
    8192,
    2<<14,
    2<<15,
    2<<16,
    2<<31 // this is the maximum length of a jdbm record.
    };

    /**
     * An alternative set of histogram bin declarations used for a
     * more fine grained analysis of the heap.
     */

    public static final int[] fineGrainBinDecls = new int[] {
        2,
        4,
        8,
        16,	 18,	  20,	  22,	  24,	  26,	  28,	30,
        32,	 36,	  40,	  44,	  48,	  52,	  56,	60,
        64,	 72,	  80,	  88,	  96,	 104,	 112,	120,
        128,	144,	 160,	 176,	 192,	 208,	 224,	240,
        256,	288,	 320,	 352,	 384,	 416,	 448,	480,
        512,    576,	 640,	 704,	 768,	 832,	 896,	960,
        1024,  1152,	1280,	1408,	1536,	1664,	1792,	1920,
        2048,  2304,	2560,	2816,	3072,	3328,	3584,	3840,
        4096,  4608,	5120,	5632,	6144,	6656,	7168,	7680,
        8192,  9216,   10240,  11264,  12288,  13312,  14336,  15360, // 8192 is the block size.
        2<<14,
        2<<15,
        2<<16,
        2<<31 // this is the maximum length of a jdbm record.
    };

    /**
     * Command line utility for dumping the state of the jdbm store.
     * 
     * @param args <pre>[options] <i>filename</i>
     * 
     * options:
     * 
     *    -A    All (equivilent to -F -T -U -L -P).
     *    -F    Summarize the free page list.
     *    -T	Summarize the translation page list (includes record type breakdown).
     *    -U	Summarize the used page list (default if no options are given, includes
     *          deleted record summary).
     *    -L	Summarize the free logical row id page list.
     *    -P    Summarize the free physical row id page list.
     * 
     *    -fg   Use fine-grained histograms.
     *
     *    -ss   Scan for strings in data.
     *    -ssi  Case insensitive string scan (default is case sensitive).
     *    -ssml# Minimum length for scanned strings to # (default is 5).
     *    -ssML# Maximum length for scanned strings to # (default is 80).
     *    -ssmf# Minimum frequency count to report for scanned strings to # (default is 2).
     * 
     *    -pm#  Sets the page marker count to #.
     *    -pl#  Sets the page limit count to #.
     * 
     *    -ra{cl} Names the {@link IRecordAnalyzer} implementation class.
     * 
     * filename - the base filename of a pre-existing jdbm store.
     * 
     * </pre>
     */
    
    public static void main( String[] args )
    	throws IOException
    {

        String filename = null;
        
        boolean defaultSummary = true;
        boolean summarizeFreePageList = false;
        boolean summarizeTranslationPageList = false;
        boolean summarizeUsedPageList = false;
        boolean summarizeFreeLogicalRowIdPageList = false;
        boolean summarizeFreePhysicalRowIdPageList = false;
        
        boolean fineGrained = false;
	
        boolean scanStrings = false;
	boolean scannerCaseSensitive = true;
	int     scannerMinLength = 5;
	int     scannerMaxLength = 80;
	int     scannerMinFreq = 2;

	int pageMarkerCount = 10;
	int pageLimitCount = 0;

	String recordAnalyzer = null;
	
        for( int i=0; i<args.length; i++ ) {
        
            String arg = args[ i ];
            
            if( arg.startsWith( "-" ) ) {

                if( arg.equals( "-A") ) {
                    
                    defaultSummary = false;
                    
                    summarizeFreePageList = true;
                    summarizeTranslationPageList = true;
                    summarizeUsedPageList = true;
                    summarizeFreeLogicalRowIdPageList = true;
                    summarizeFreePhysicalRowIdPageList = true;
                    
                } else if( arg.equals( "-F") ) {
                    
                    defaultSummary = false;

                    summarizeFreePageList = true;
                    
                } else if( arg.equals( "-T") ) {
                        
                        defaultSummary = false;

                        summarizeTranslationPageList = true;
                        
                } else if( arg.equals( "-U" ) ) {
                    
                    defaultSummary = false;
                    
                    summarizeUsedPageList = true;
                    
                } else if( arg.equals( "-L" ) ) {
                    
                    defaultSummary = false;
                    
                    summarizeFreeLogicalRowIdPageList = true;
                    
                } else if( arg.equals( "-P" ) ) {
                    
                    defaultSummary = false;
                    
                    summarizeFreePhysicalRowIdPageList = true;

                } else if( arg.equals( "-fg" ) ) {
                    
                    fineGrained = true;
                    
                } else if( arg.equals( "-ss" ) ) {

		    scanStrings = true;
		    
                } else if( arg.equals( "-ssi" ) ) {

		    scannerCaseSensitive = false;
		    
                } else if( arg.startsWith( "-ssml" ) ) {

		    scannerMinLength = Integer.parseInt( arg.substring(5) );
		    
                } else if( arg.startsWith( "-ssML" ) ) {

		    scannerMaxLength = Integer.parseInt( arg.substring(5) );
		    
                } else if( arg.startsWith( "-ssmf" ) ) {

		    scannerMinFreq = Integer.parseInt( arg.substring(5) );
		    
                } else if( arg.startsWith( "-pm" ) ) {

                    String tmp = arg.substring( 3 );

                    pageMarkerCount = new Integer( tmp ).intValue();
                    
                } else if( arg.startsWith( "-pl") ) {

                    String tmp = arg.substring( 3 );

                    pageLimitCount = new Integer( tmp ).intValue();

                } else if( arg.startsWith( "-ra") ) {
                    
                    String tmp = arg.substring( 3 );
                    
                    recordAnalyzer = tmp;
                    
		} else {
                
                    usage( "Do not understand: "+arg, args );
                
                }
                
            } else {
                
                filename = arg;
                
            }
            
        }
        
        if( filename == null ) {
            
            usage( "Must specify filename.", args );
            
        }

        if( defaultSummary ) {
            
            summarizeUsedPageList = true;
            
        }

        //
        // Instantiate the dump utility class.
        //
        
        Properties properties = new Properties();
        
        properties.setProperty
            ( RecordManagerOptions.DUMP,
              "true"
              );
        
        RecordManager recman = RecordManagerFactory.createRecordManager
            ( filename,
              properties
              );
        
        // The eventual delegate is the DumpUtility.
        DumpUtility du = (DumpUtility) recman.getBaseRecordManager();

//        // Cause the extensible serializer to be cached to avoid problems
//        // with "double get for block xxx" if we try to apply this serializer
//        // to interpret records in a block while we have a lock on that block.
//        du.getDefaultSerializer().serialize( recman, new String() );

        du.setPageLimit( pageLimitCount );
        
        du.setPageMarkerCount( pageMarkerCount );

        if( recordAnalyzer != null ) {

            try {
                du.setRecordAnalyzer( (IRecordAnalyzer) Class.forName(recordAnalyzer).newInstance() );
            }
            catch( Throwable t ) {
                t.printStackTrace( System.err );
                usage( "Could not establish record analyzer: class="+recordAnalyzer, args );
            }
        }
        
        if( fineGrained ) {
            
            du.setHistogramBins( fineGrainBinDecls );
        
        }

        if( summarizeFreePageList ) {
            
            du.writeFreePageListSummary( System.out );
            
        }
        
        if( summarizeTranslationPageList ) {

            StringScanner scanner = 
		( scanStrings
		  ? new StringScanner(
		  	scannerCaseSensitive,
			scannerMinLength,
			scannerMaxLength,
			scannerMinFreq
		  	)
		  : null
		  );

            du.writeTranslationPageListSummary( System.out, scanner );
            
        }
        
        if( summarizeUsedPageList ) {

            du.writeUsedPageListSummary( System.out );
            
        }

        if( summarizeFreeLogicalRowIdPageList ) {
            
            du.writeFreeLogicalRowIdPageListSummary( System.out );
            
        }

        if( summarizeFreePhysicalRowIdPageList ) {
            
            du.writeFreePhysicalRowIdPageListSummary( System.out );
            
        }

        du.close();
        
        System.exit( 0 );
                
    }
    
    /**
     * Applications may implement this interface and supply the class name of
     * their implementation to the {@link DumpUtility}in order to provide a
     * more detailed or customized analysis of records in the store.
     * 
     * @author thompsonbry
     */
    
    public interface IRecordAnalyzer
    {
        
        /**
         * Return an identifier that is used to classify the record as being of
         * a certain type. Since jdbm does not record a unique record type or
         * class identifier with each record this method is heuristic.
         * 
         * @param data
         *            The data from some record.
         * 
         * @return The record type identifier, e.g., a class name.
         */

        public String getRecordType( DumpUtility dump, long recid, byte[] data )
            throws IOException;
        
    }

    /**
     * Default implementation. Exposes some utility methods that might be of
     * interest to application which implement their own {@link IRecordAnalyzer}.
     * <p>
     * 
     * The following jdbm classes implement {@link Externalizable}, which
     * causes the class name to be serialized. We can identify instances of
     * these classes in the store.
     * <ul>
     * <li>{@link BTree}</li>
     * <li>{@link HashDirectory}</li>
     * <li>{@link HashBucket}</li>
     * <li>{@link ExtensibleSerializer}</li>
     * </ul>
     * <p>
     * 
     * The following classes implement {@link Serializer}, which leaves fewer
     * clues: {@link BPage}. There is no guarenteed way to recognize instances
     * of {@link BPage}, but if there is only one "unknown" category it is
     * probably {@link BPage}. Unfortunately we can't simply "try" the BPage
     * deserializer routine since it requires access to the parent BTree in
     * order to get the #of keys per node and several other critical BTree
     * specific fields. Also {@link BPage}does not have any easily identifiable
     * leading byte pattern.
     * <p>
     * 
     * The only thing is jdbm that uses full Java Serialization is the named
     * dictionary maintained by the {@link BaseRecordManager}.
     * <p>
     * 
     * @author thompsonbry
     */
    
    public static class DefaultRecordAnalyzer
    	implements IRecordAnalyzer
    {

        /**
         * Attempts to deserialize the record. This succeeds if the record was
         * serialized using the default serializer for the store.
         * 
         * @param dump
         * @param recid
         * @param serialized
         * 
         * @return The deserialized record and <code>null</code> iff the
         *         record COULD NOT be deserialized.
         * 
         * @throws IOException
         */
        
        public Object deserialize( DumpUtility dump, long recid, byte[] serialized )
            throws IOException
        {
            
            ISerializationHandler defaultSerializer = dump.getSerializationHandler();
        
            try {
                
                /* Try to deserialize the object using that serializer. This can
                 * fail since some objects, e.g., BPage, use their own serializers.
                 */

                Object obj = defaultSerializer.deserialize
                    ( dump, recid, serialized
                      );

                return obj;
                    
            }

            catch( Throwable t ) {

                // Could not deserialize this object.

                if( ! ( defaultSerializer instanceof DefaultSerializer ) ) {
                    
                    /*
                     * This case applies when the default serialization handler is
                     * not not using Java serialization, e.g., when the extensible
                     * serializer is used.
                     */
                    
                    try {
                    
                        Object obj = DefaultSerializer.INSTANCE.deserialize
                            ( serialized
                              );

                        return obj;
                        
                    }
                    
                    catch( Throwable t2 ) {
                        
                        // Probably a BPage.

                        return null;

                    }
                    
                }
                
                // Probably a BPage.

                return null;
                
            }
            
        }

        /**
         * Attempt to guess the class. This works for objects written using an
         * {@link java.io.ObjectOutputStream}. The first 6 bytes are a stream
         * header. We skip the header and then extract the class name.
         * <p>
         */
        
        public String guessClassName( byte[] serialized )
            throws IOException
        {
                    
            if( serialized.length < 8 ) {
                
                // Some sort of odd super small record which can't possible
                // be using Serializable or Externalizable for persistence.
                
                return "<tiny:"+serialized.length+">";
                
            }
            
//            ObjectInputStream ois = new ObjectInputStream
//            ( new BufferedInputStream
//                 ( new ByteArrayInputStream( data ),
//                   data.length
//                   )
//              );

            DataInputStream ois = new DataInputStream( new ByteArrayInputStream( serialized ) );
            
//         dump( ps, data, 128 );
         
//         ois.mark( data.length );
//         
//         try {
      
            for( int i=0; i<6; i++ ) ois.read(); // skip something.
            
            int nchars = ois.readUnsignedShort();
            
            if( nchars == 0 ) {
                
                return "<unknown>";
                
            }
            
            if( serialized.length - 8 - nchars < 0 ) {
                
                // There would not be enough data to read this many chars.
                
                return "<unknown2>";
                
            }
            
            char[] chars = new char[ nchars ];
            
            int j = 0;
            
            for( int i=0; i<nchars; i++ ) {
                
                char ch = (char) ois.readUnsignedByte();
                
                if( ch >= 32 && ch < 127 ) {
                
                    // Only "ascii" characters since junk otherwise gets
                    // in when the bytes are not really characters at all.

                    chars[ j++ ] = ch;
                    
                }
                
            }
            
            String cname = new String( chars );
                 
            ois.close();
            
            return cname;
            
        }

        /**
         * Return an identifier that is used to classify the record as
         * being of a certain type.  Since jdbm does not record a unique
         * record type or class identifier with each record this method is
         * heuristic.
         * 
         * @param data The data from some record.
         * 
         * @return The record type identifier, e.g., a class name.
         */

        public String getRecordType( DumpUtility dump, long recid, byte[] data )
        	throws IOException
        {

            Object obj = deserialize( dump, recid, data );
            
            if( obj != null ) {

                String name = obj.getClass().getName();
                
                return name;
                    
            }

            return guessClassName( data );
            
        }

    }
    
}
