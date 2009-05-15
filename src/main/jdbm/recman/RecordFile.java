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
 * $Id: RecordFile.java,v 1.13 2006/06/03 18:22:46 thompsonbry Exp $
 */

package jdbm.recman;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Iterator;
import java.util.LinkedList;

import jdbm.helper.CacheEvictionException;
import jdbm.helper.CachePolicyListener;
import jdbm.helper.MRUNativeLong;
import jdbm.helper.Serializer;
import jdbm.helper.maps.LongKeyChainedHashMap;
import jdbm.helper.maps.LongKeyMap;

/**
 * This class represents a random access file as a set of fixed size records
 * known as <em>blocks</em> or <em>pages</em>. Each record has a physical
 * record number (<em>blockid</em>), and records are cached in order to
 * improve access. The size of a block is specified by {@link #BLOCK_SIZE}. A
 * block is modeled by {@link BlockIo}which provides an in-memory copy of the
 * state of the block on disk and also maintains some metadata about the state
 * of the block.
 * <p>
 * 
 * The {@link RecordFile}uses a write-ahead logging strategy and maintains a
 * separate <em>data file</em> and <em>log file</em>. Dirty pages are
 * accumulated during a transaction and transactions are written to disk by
 * {@link RecordFile#commit()}. The advantage of the write-ahead logging
 * strategy is that writes against the log are continguous and can therefore
 * take advantage of higher potential IO bandwidth. In contrast updates against
 * the data file are random and have higher seek latency and lower potential IO
 * bandwidth.
 * <p>
 * 
 * Nothing is written onto the data file until {@link RecordFile#commit()}is
 * invoked. If transactions are disabled, then {@link RecordFile#commit()}
 * causes the dirty pages to be updated directly against the data file.
 * Otherwise the dirty pages constituting the current transaction are written on
 * the log. In either case, the <i>dirty </i> flag for each dirty page is
 * cleared during {@link RecordFile#commit()}and all entries are removed from
 * the dirty list. If a crash occurs while there are transaction(s) on the log,
 * then the state of those transaction(s) is recovered on startup, applied to
 * the data file, and the log file is deleted.
 * <p>
 * 
 * From time to time during normal operation the transactions in the log are
 * processed, which involves updating the relevant pages in the data file and
 * deleting the log file. Writes on the data file are aggregated across
 * transactions and ordered in an attempt to minimize redundent updates and
 * improve effective throughput.
 * <p>
 * 
 * The <em>free</em> list is an allocation cache for {@link BlockIo}objects.
 * Its size is capped by a configurable parameter.
 * <p>
 * 
 * The <em>clean</em> list contains those pages which have been fetched from
 * the store and whose state has NOT been modified. The clean list is
 * implemented as a MRU based on a hash map. Pages that fall off of the end of
 * the clean list are migrated to the "free" list for reuse. The size of the
 * clean list is capped by a configurable parameter.
 * <p>
 * 
 * The <em>in-use</em> list contains those pages on which a
 * {@link RecordFile#get(long blockid )}has been performed and which have not
 * been {@link RecordFile#release(BlockIo block) released}.
 * {@link RecordFile#get(long blockid)}returns a {@link BlockIo}. Normally a
 * {@link RecordFile#get(long blockid)}is wrapped in a <code>try</code> block
 * and the <code>finally</code> clause performs the release. As an alternative
 * {@link RecordFile#release(long blockid, boolean isDirty)}is available to
 * indicate that the block is dirty and should be placed onto the <em>dirty</em>
 * list. The in-use list is used to detect "double get"s in an effort to eagerly
 * identify attempts to fetch more than one copy of the same block, which are
 * regarded as logic errors.
 * <p>
 * 
 * The <em>dirty</em> list is a list of pages whose state has been updated but
 * not yet written to disk (either to the log if using transactions or to the
 * data file if not using transactions). The dirty list grows during a
 * transaction until {@link RecordFile#commit()}, at which point the dirty
 * pages are accumulated into a transaction which is written onto the log file.
 * <p>
 * 
 * The <em>in-txn</em> list is a list of pages within historical
 * transaction(s) that have not yet been updated against the data file. This
 * list is populated each time another transaction is committed by
 * {@link RecordFile#commit()}. The list is cleared only when the log is
 * updated against the data file. However, individual pages are removed from the
 * transaction list by {@link #get(long blockid)}and migrated back to the
 * transaction list by {@link #release(BlockIo block)}as part of an effort to
 * make sure that any given page appears on at most one of the page lists: {
 * free, dirty, in-use, in-transaction}.
 * <p>
 * 
 * Some questions:
 * <ul>
 * 
 * <li>An iterator is used over the dirty page list during a commit. The
 * javadoc for {@link HashMap}indicates that iterators can be inefficient for a
 * hash map since all hash entries must be visited (even those which are null)
 * and then the chain on each entry must be visited. It would be more efficient
 * to link the pages on the dirty list together so that we could chase those
 * references instead.</li>
 * 
 * <li>Based on what I can read into the code, the practice of periodically
 * forcing a commit when transactions are NOT in use means that the dirty pages
 * are flushed to the data file and added to the free list (see my comments on
 * the free list above). Doing this means that you can not rollback the
 * transactions, and in fact an exception is thrown if you try to do this.
 * However, if we were NOT periodically flushing the dirty pages to disk, then
 * you could rollback the transaction since nothing is written onto the disk
 * until {@link RecordFile#commit()}. This goes back to the question about the
 * semantics of the {@link jdbm.RecordManagerOptions#DISABLE_TRANSACTIONS}
 * option. As the code stands, this both disables the write-ahead logging
 * strategy and invalidates the use of rollback. I think that these should be
 * distinct options. I.e., let's define an "auto-commit" option which
 * invalidates rollback once an auto-commit has been performed within a
 * transaction and clarify that the "disable transaction" option disables the
 * write-ahead logging strategy, but the current transaction is still present on
 * the dirty list and may be rolled back unless the auto-commit option has been
 * specified with a non-zero size (in pages) and an auto-commit has been
 * performed during the transaction. In fact, the use of auto-commit with
 * transactions enabled is equally valid since it flushes the dirty pages to the
 * log and migrates them to the in-transaction page list. So, auto-commit would
 * be a memory management option that sacrifices the ability to rollback a
 * transaction once a threshold amount of pages have been buffered in exchange
 * for placing a cap on the size of the dirty list. Note that I think that the
 * auto-commit value should be the size of the dirty list, which has a direct
 * interpretation as N pages worth of heap space. Another drawback is that the
 * code is not capping the size of the free list, so dirty pages without write
 * ahead logging can wind up on the free list instead of being released back to
 * the heap. In addition, the #of transaction buffers would be part of a memory
 * management strategy.</li>
 * 
 * <li>{@link jdbm.recman.BlockIo#incrementTransactionCount()}is supposed to
 * snapshot the block (javadoc), but it does not do this and is marked as a
 * "fixme" for alex.</li>
 * 
 * <li>Each transaction "buffer" in the {@link TransactionManager}is an array
 * list whose values are the dirty pages (by reference) in that transaction. A
 * new list of the dirty pages is created by {@link TransactionManager#start()}.
 * {@link RecordManager#commit()}scans the dirty page list, removes each dirty
 * page and adds it to the current transaction buffer using
 * {@link TransactionManager#add(BlockIo block)}.
 * {@link RecordManager#commit()}finally invokes
 * {@link TransactionManager#commit()}, which uses default Java serialization
 * (ouch) to write the array list of dirty pages onto the log file. The object
 * output stream and the underlying file output stream are then flushed to the
 * log file which is synched to disk. However, the transaction buffer is NOT
 * discarded, which means that jdbm is holding hard references to all blocks in
 * up to N historical transactions. The references to those blocks held by the
 * transaction buffers are not released until the log is applied to the data
 * file.
 * <p>
 * 
 * The comments mention a thread which is responsible for updating the data file
 * from the log, but I can not identify any such thread in jdbm. Instead it
 * appears that {@link TransactionManager#start()}does this synchronously when
 * there are no more transaction buffers, which could result in a large latency
 * for the application. Further, since the transaction buffers are lists of
 * references to the dirty pages, the state of those pages may be more current
 * than their state when the transaction was committed, which means that the
 * synchronization of the data file from the transaction log buffers could be
 * inconsistent (and probably is unless the code manages to ensure that those
 * blocks are NOT also used by the {@link RecordFile}).
 * <p>
 * 
 * In fact, {@link RecordFile#commit()}adds dirty pages to the
 * <em>in-transaction</em> list, which is tested by
 * {@link RecordFile#get(long blockid)}. This means that a {@link BlockIo}
 * instance that is part of the transaction log can, in fact, re-enter use
 * within a subsequent transaction. If it is then modified within that
 * subsequent transaction (there is no protection against this), then the
 * transaction isolation has failed and the update from the in memory log
 * buffers to the data file will result in an inconsistent state for the data
 * file. (I think that this could be fixed by a lazy copy of the state of the
 * block based on the transaction counter, which appears to have been the
 * original intention.)
 * <p>
 * 
 * There exists code to synchronize the data file from the disk log, and that
 * code is in fact used for recovery on startup. I would suggest that we could
 * create a runtime option to perform synchronization from the on disk log and
 * improve the log file format and serialization to make this more efficient. We
 * could use a new magic number for the new log format or insist that people
 * recover any log files before migrating (or both).
 * <li>
 * 
 * <li>Should we offer an option to sync on commit when transactions are
 * disabled?</li>
 * 
 * <li>FIXME If there was a crash while writing to the log or if the log file
 * is otherwise corrupt then recovery process will ignore the rest of the log
 * and does NOT issue a warning. I would suggest that we require an explicit
 * option for this behavior ("forceRecovery") and otherwise terminate the
 * application with a warning and without deleting the log file.</li>
 * 
 * <li>There is a discussion on the <a
 * href="mailto:jdbm-developer@users.sourceforge.net">developers mailing list
 * </a> in which we are considering replacing the {@link RecordFile}and
 * {@link TransactionManager}with an implementation of DBCache. DBCache is a
 * high-throughput parallel transaction model with a design well suited to the
 * recman layer of jdbm.</li>
 * 
 * </ul>
 * 
 * @see TransactionManager
 */
public final class RecordFile
    implements CachePolicyListener
{
    
    final TransactionManager txnMgr;

    /**
     * Size of the MRU consisting of blocks that are clean (recently
     * read and not modified since).  Blocks that fall off of the MRU
     * are moved to the free list or discarded if the free list is full.
     */
    private int cleanMRUSize = 1000;

    /**
     * Maximum #of blocks on the "free" list. The free list is an allocation
     * cache for {@link BlockIo} objects.
     */
    private int maxFreeSize = 100;

    /**
     * Effects behavior when transactions are enabled and results in eager
     * partial commit of the transaction.
     */
    private int maxDirtySize = 10000;

    // Todo: reorganize in hashes and fifos as necessary.
    // free -> inUse -> dirty -> inTxn -> free
    // free is a cache, thus a FIFO. The rest are hashes.
    
    /**
     * Allocation cache for {@link BlockIo}.
     */
    private final LinkedList free = new LinkedList();

    /**
     * Blocks that are clean (vs dirty) to avoid re-reads of clean blocks
     * (blocks whose state in current with the state of the block on disk). MRU
     * policy governs ejection of LRU pages from this cache (to the free list or
     * the GC depending on the size of the free list).
     * <P>
     * Note: The {dirty,serializer} metadata maintained by the object CachePolicy
     * are ignored for the purposes of the RecordFile cache.
     */
    private MRUNativeLong clean = new MRUNativeLong( cleanMRUSize );
    
    /**
     * Blocks currently locked for read/update ops. When released the block goes
     * to the dirty or clean list, depending on a flag.  The file header block is
     * normally locked plus the block that is currently being read or modified.
     * 
     * @see BlockIo#isDirty()
     */
    private final LongKeyMap inUse = new LongKeyChainedHashMap();

    /**
     * Blocks whose state is dirty.
     */
    private final LongKeyMap dirty = new LongKeyChainedHashMap();
    /**
     * Blocks in a <em>historical</em> transaction(s) that have been written
     * onto the log but which have not yet been committed to the database.
     */
    private final LongKeyMap inTxn = new LongKeyChainedHashMap();
    
    // transactions disabled?
    private boolean transactionsDisabled = false;
    
    /** sync file on close if transactions are disabled - this ensures that
     *  all data is actually persisted to physical disk when the file is closed
     *  (and not just to the operating system disk cache.  Default is true.
     */
    private boolean syncOnClose = true;
    
    /** The length of a single block. */
    public final static int BLOCK_SIZE = 8192; //16384; //4096;

    /** The extension of a record file */
    final static String extension = ".db";

    /** A block of clean data to wipe clean pages. */
    final static byte[] cleanData = new byte[BLOCK_SIZE];

    private RandomAccessFile file;
    private final String fileName;

    /**
     *  Creates a new object on the indicated filename. The file is
     *  opened in read/write mode.
     *
     *  @param fileName the name of the file to open or create, without
     *         an extension.
     *  @throws IOException whenever the creation of the underlying
     *          RandomAccessFile throws it.
     */
    RecordFile(String fileName) throws IOException {
        this.fileName = fileName;
        if( new File( fileName + extension ).exists() ) {
        	System.err.println("INFO: database exists: "+fileName);
        }
        file = new RandomAccessFile(fileName + extension, "rw");
        txnMgr = new TransactionManager(this);
        clean.addListener( this );
    }

    
    /**
     *  Returns the file name.
     */
    String getFileName() {
        return fileName;
    }

    /**
     *  Disables transactions: doesn't sync and doesn't use the
     *  transaction manager.
     */
    void disableTransactions(int maxDirtySize, boolean syncOnClose) {
        transactionsDisabled = true;
        this.maxDirtySize = maxDirtySize;
        this.syncOnClose = syncOnClose;
    }

    void disableTransactions(int maxDirtySize) {
        disableTransactions(maxDirtySize, syncOnClose);
    }
    
    void disableTransactions() {
        disableTransactions(maxDirtySize);
    }

    /**
     * Set the size of the MRU of "clean" blocks.  This method MUST NOT
     * be invoked once the {@link RecordFile} enters use.
     * 
     * @param val The size of the MRU.
     */

    void setCleanMRUCapacity( int val ) {
        cleanMRUSize = val;
        clean = new MRUNativeLong( cleanMRUSize );
    }
    
    /**
     * Set the maximum capacity of the list of "free" blocks.
     * 
     * @param val The maximum capacity of the "free" list.
     */
    
    void setFreeListCapacity( int val ) {
        maxFreeSize = val;
    }
    
    /**
     * When non-zero, a set of counters will be displayed event N events. An
     * event is a fetch from disk, a write to disk, or a logical extension of
     * the file for a new block. This information may prove useful when
     * profiling the store.  Reasonable values are zero (disabled) and 1000+.
     * 
     * @param val The counter display rate or zero (0) to disable.
     */
    
    void setCounterDisplayRate( long val ) {
        triggerRate = val;
    }
    
    /**
     *  Gets a block from the file. The returned byte array is
     *  the in-memory copy of the record, and thus can be written
     *  (and subsequently released with a dirty flag in order to
     *  write the block back).
     *
     *  @param blockid The record number to retrieve.
     */
     BlockIo get(long blockid) throws IOException {
         long key = blockid;

         // try in transaction list, dirty list, clear, free list
         BlockIo node = (BlockIo) inTxn.get(key);
         if (node != null) {
             inTxn.remove(key);
             inUse.put(key, node);
             return node;
         }
         node = (BlockIo) dirty.get(key);
         if (node != null) {
             dirty.remove(key);
             inUse.put(key, node);
             return node;
         }
         node = (BlockIo) clean.get( new Long( key ) );
         if( node != null ) {
             clean.remove( new Long( key ) );
             cleanBlocksHitCount++;
             inUse.put( key, node );
             return node;
         }
// Do NOT check the free list -- it is just an allocation cache.         
//         for (Iterator i = free.iterator(); i.hasNext(); ) {
//             BlockIo cur = (BlockIo) i.next();
//             if (cur.getBlockId() == blockid) {
//                 node = cur;
//                 i.remove();
//                 inUse.put(key, node);
//                 return node;
//             }
//         }

         // sanity check: can't be on in use list
         if (inUse.get(key) != null) {
             throw new Error("double get for block " + blockid);
         }

         // get a new node and read it from the file
         node = getNewNode(blockid);
         long offset = blockid * BLOCK_SIZE;
         if (file.length() > 0 && offset <= file.length()) {
             // read a block from disk.
             read(file, offset, node.getData(), BLOCK_SIZE);
             fetchBlockCount++;
             showCounters();
         } else {
              // get beyond the end of the data file uses a blank page rather
              // extending the file.
              System.arraycopy(cleanData, 0, node.getData(), 0, BLOCK_SIZE);
              extendBlockCount++;
              showCounters();
         }
         inUse.put(key, node);
         node.setClean();
         return node;
     }


    /**
     *  Releases a block.
     *
     *  @param blockid The record number to release.
     *  @param isDirty If true, the block was modified since the get().
     */
    void release(long blockid, boolean isDirty)
    throws IOException {
        BlockIo node = (BlockIo) inUse.get(blockid);
        if (node == null)
            throw new IOException("bad blockid " + blockid + " on release");
        if (!node.isDirty() && isDirty)
            node.setDirty();
        release(node);
    }

    /**
     *  Releases a block.
     *
     *  @param block The block to release.
     * @throws IOException
     */
    void release(BlockIo block) throws IOException {
        long key = block.getBlockId();
        inUse.remove(key);
        if (block.isDirty()) {
            // System.out.println( "Dirty: " + key + block );
            dirty.put(key, block);
            if (transactionsDisabled && dirty.size() > maxDirtySize)
            	commit();
        } else {
            if (!transactionsDisabled && block.isInTransaction()) {
                inTxn.put(key, block);
            } else {
                putClean( key, block );
//                free.add(block);
            }
        }
    }

    /**
     * Puts a block on the "clean" MRU.  This can cause LRU clean
     * blocks to be ejected to the "free" list.
     * 
     * @param key
     * @param block
     */
    
    private void putClean( long key, BlockIo block )
    {
        try {
            clean.put( new Long(key), block, false, null ); // {dirty,serializer} are ignored.
        } catch( CacheEvictionException ex ) {
            // masquerade exception until we replace the MRU impl.
            throw new RuntimeException( ex );
        }   
    }

    /**
     *  Discards a block (will not write the block even if it's dirty)
     *
     *  @param block The block to discard.
     */
    void discard(BlockIo block) {
        long key = block.getBlockId();
        inUse.remove(key);

        // note: block not added to free list on purpose, because
        //       it's considered invalid
    }

    /**
     *  Commits the current transaction by flushing all dirty buffers
     *  to disk.
     */
    void commit() throws IOException {
        // debugging...
        if (!inUse.isEmpty() && inUse.size() > 1) {
            showList(inUse.values().iterator());
            throw new Error("in use list not empty at commit time ("
                            + inUse.size() + ")");
        }

        //  System.out.println("committing...");

        if ( dirty.size() == 0 ) {
            // if no dirty blocks, skip commit process
            return;
        }

        if( triggerRate != 0 ) showCounters( false );

        if (!transactionsDisabled) {
            txnMgr.start();
        }

        for (Iterator i = dirty.values().iterator(); i.hasNext(); ) {
            BlockIo node = (BlockIo) i.next();
            i.remove();
            // System.out.println("node " + node + " map size now " + dirty.size());
            if (transactionsDisabled) {
                // update the page in the data file.
                long offset = node.getBlockId() * BLOCK_SIZE;
                file.seek(offset);
                file.write(node.getData());
                writeBlockCount++;
                showCounters();
                node.setClean();
                putClean( node.getBlockId(), node );
                //free.add(node);
            }
            else {
		// add the page to the transaction buffer.
                txnMgr.add(node);
                inTxn.put(node.getBlockId(), node);
            }
        }
        if (!transactionsDisabled) {
            // write the transaction buffer to the log file.
            txnMgr.commit();
        }

        if( triggerRate != 0 ) showCounters( true );

    }

    /**
     *  Rollback the current transaction by discarding all dirty buffers
     */
    void rollback() throws IOException {
        
    	if (transactionsDisabled)
    		throw new IOException("Rollback not allowed if transactions are disabled");
    	
    	// debugging...
        if (!inUse.isEmpty()) {
            showList(inUse.values().iterator());
            throw new Error("in use list not empty at rollback time ("
                            + inUse.size() + ")");
        }
        //  System.out.println("rollback...");
        dirty.clear();

        txnMgr.synchronizeLogFromDisk();

        if (!inTxn.isEmpty()) {
            showList(inTxn.values().iterator());
            throw new Error("in txn list not empty at rollback time ("
                            + inTxn.size() + ")");
        };
    }

    /**
     *  Commits and closes file.
     */
    void close() throws IOException {
        if (!dirty.isEmpty()) {
            commit();
        }
        txnMgr.shutdown();

        if (!inTxn.isEmpty()) {
            showList(inTxn.values().iterator());
            throw new Error("In transaction not empty");
        }

        // these actually ain't that bad in a production release
        if (!dirty.isEmpty()) {
            System.out.println("ERROR: dirty blocks at close time");
            showList(dirty.values().iterator());
            throw new Error("Dirty blocks at close time");
        }
        if (!inUse.isEmpty()) {
            System.out.println("ERROR: inUse blocks at close time");
            showList(inUse.values().iterator());
            throw new Error("inUse blocks at close time");
        }

        // debugging stuff to keep an eye on the free list
        // System.out.println("Free list size:" + free.size());
        
        if (transactionsDisabled && syncOnClose)
            sync();
        
        file.close();
        file = null;
    }


    /**
     * Force closing the file and underlying transaction manager.
     * Used for testing purposed only.
     */
    void forceClose() throws IOException {
      txnMgr.forceClose();
      file.close();
    }

    /**
     *  Prints contents of a list
     */
    private void showList(Iterator i) {
        int cnt = 0;
        while (i.hasNext()) {
            System.out.println("elem " + cnt + ": " + i.next());
            cnt++;
        }
    }


    /**
     *  Returns a new node. The node is retrieved (and removed)
     *  from the released list or created new.
     */
    private BlockIo getNewNode(long blockid)
    throws IOException {

        BlockIo retval = null;
        if (!free.isEmpty()) {
            retval = (BlockIo) free.removeFirst();
            freeBlocksUsedCount++;
        }
        if (retval == null)
            retval = new BlockIo(0, new byte[BLOCK_SIZE]);

        retval.setBlockId(blockid);
        retval.setView(null);
        return retval;
    }

    /**
     *  Synchs a node to disk. This is called by the transaction manager's
     *  synchronization code.
     */
    void synch(BlockIo node) throws IOException {
        byte[] data = node.getData();
        if (data != null) {
            long offset = node.getBlockId() * BLOCK_SIZE;
            file.seek(offset);
            file.write(data);
            writeBlockCount++;
            showCounters();
        }
    }

    /**
     *  Releases a node from the transaction list, if it was sitting
     *  there.
     *
     *  @param recycle true if block data can be reused
     */
    void releaseFromTransaction(BlockIo node, boolean recycle)
    throws IOException {
        long key = node.getBlockId();
        if ((inTxn.remove(key) != null) && recycle) {
            putClean( key, node );
//            free.add(node);
        }
    }

    /**
     *  Synchronizes the file.
     */
    void sync() throws IOException {
        file.getFD().sync();
    }


    /**
     * Utility method: Read a block from a RandomAccessFile
     */
    private static void read(RandomAccessFile file, long offset,
                             byte[] buffer, int nBytes) throws IOException {
        file.seek(offset);
        int remaining = nBytes;
        int pos = 0;
        while (remaining > 0) {
            int read = file.read(buffer, pos, remaining);
            if (read == -1) {
                System.arraycopy(cleanData, 0, buffer, pos, remaining);
                break;
            }
            remaining -= read;
            pos += read;
        }
    }

    //
    // counters.
    //
    
    private long cleanBlocksEvictedCount = 0L;
    private long freeBlocksAddedCount = 0L;
    private long freeBlocksUsedCount = 0L;
    private long cleanBlocksHitCount = 0L;
    private long fetchBlockCount = 0L;
    private long writeBlockCount = 0L;
    private long extendBlockCount = 0L;
    
    /**
     * When non-zero, the counters are written every N events.  Try
     * values of 1000 or so when debugging.  You can set this using
     * a configuration property in {@link Provider}.
     * 
     * @see #triggerCount
     */
    private long triggerRate = 0;
    
    /**
     * Event counter. An event is a fetch (from disk), a write (to disk), or an
     * extend operation (which is only logical and does not touch the disk).
     * 
     * @see #triggerRate
     */
    
    private long triggerCount = 0L;

    /**
     * Shows the event counters every {@link #triggerRate} events.
     */
    
    private void showCounters()
    {

        triggerCount++;
        
        if( triggerRate != 0 && triggerCount % triggerRate == 0 ) {
            
            showCounters( false );
            
        }
        
    }
    
    /**
     * Writes the current counters and other data of interest.
     * 
     * @param reset When true, the counters are reset afterwards.
     */
    
    private void showCounters(boolean reset)
    {
        
        // #of blocks held on all lists by the RecordFile (the
        // transaction manager can have more data in buffered
        // transactions).
        
        long nblocks = inTxn.size()
        	     + dirty.size()
        	     + inUse.size()
        	     + clean.size()
        	     + free.size()
        	     ;
        
        long memused = ( nblocks * BLOCK_SIZE ) / ( 1024 * 1024 );
        
        System.err.println( "memory used (mb): "+memused );
        System.err.println( "# blocks in mem : "+nblocks );
        System.err.println( "# inTxn blocks  : "+inTxn.size() );
        System.err.println( "# dirty blocks  : "+dirty.size() );
        System.err.println( "# inUse blocks  : "+inUse.size() );
        System.err.println( "# fetch blocks  : "+fetchBlockCount );
        System.err.println( "# write blocks  : "+writeBlockCount );
        System.err.println( "# extend blocks : "+extendBlockCount );
        System.err.println( "# clean blocks  : "+clean.size() );
        System.err.println( "# clean hit     : "+cleanBlocksHitCount );
        System.err.println( "# clean evicted : "+cleanBlocksEvictedCount );
        System.err.println( "# free blocks   : "+free.size() );
        System.err.println( "# free added    : "+freeBlocksAddedCount );
        System.err.println( "# free used     : "+freeBlocksUsedCount );
        System.err.println( "-----------------------------------\n");

        if(reset) resetCounters();
        
    }

    /**
     * Resets the counters (typically done in commit).
     */
    private void resetCounters() {
        cleanBlocksEvictedCount = 0L;
        freeBlocksAddedCount = 0L;
        freeBlocksUsedCount = 0L;
        cleanBlocksHitCount = 0L;
        fetchBlockCount = 0L;
        writeBlockCount = 0L;
        extendBlockCount = 0L;
        triggerCount = 0L;
    }
    
    /**
     * Used to migrate LRU pages from the "clean" list to the "free"
     * list.  If the free list reaches capacity, blocks evicted from
     * the "clean" list will be eventually swept by the JVM.
     * @param obj
     * @throws CacheEvictionException
     */
    public void cacheObjectEvicted(Object key, Object obj, boolean dirty, Serializer ser) throws CacheEvictionException {
        /*
         * Note: Eviction notices are fired by the "clean" MRU.  Those notices carry
         * additional metadata defined by the CachePolicy interface that are ignored
         * for the purposes of the RecordFile class.
         */
        cleanBlocksEvictedCount++;
        if( free.size() < maxFreeSize ) {
            freeBlocksAddedCount++;
            free.add( (BlockIo) obj );
        }
    }
    
}
