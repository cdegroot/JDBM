
package jdbm;

import jdbm.RecordManager;
import jdbm.htree.HTree;
import java.io.IOException;

/**
 * Sample JDBM application to demonstrate the use of basic JDBM operations
 *
 * @author <a href="mailto:boisvert@intalio.com">Alex Boisvert</a>
 * @version $Id: CrashTest.java,v 1.7 2002/05/31 06:34:29 boisvert Exp $
 */
public class CrashTest
{
    private RecordManager _recman;
    private HTree _htree;

    public CrashTest()
    {
        try {
            _recman = RecordManagerFactory.createRecordManager( "crashtest" );

            // create or reload HTree
            long recid = _recman.getNamedObject( "htree" );
            if ( recid == 0 ) {
                _htree = HTree.createInstance( _recman );
                _recman.setNamedObject( "htree", _htree.getRecid() );
            } else {
                _htree = HTree.load( _recman, recid );
            }

            checkConsistency();

            while (true) {
                Integer countInt = (Integer)_htree.get("count");
                if (countInt == null) {
                    System.out.println("Create new crash test");
                    countInt = new Integer(0);
                }
                int count = countInt.intValue();

                System.out.print(","+count);
                System.out.flush();

                int mod = count % 2;
                int delete_window = 20;
                int update_window = 10;

                if ((mod) == 0) {
                    // create some entries
                    for (int i=0; i<10; i++) {
                        String id = " "+count+"-"+i;
                        _htree.put("key"+id, "value"+id);
                    }

                    // delete some entries
                    if (count > delete_window) {
                        for (int i=0; i<10; i++) {
                            String id = " "+(count-delete_window)+"-"+i;
                            _htree.remove("key"+id);
                        }
                    }
                } else if ((mod) == 1) {
                    if (count > update_window+1) {
                        // update some stuff
                        for (int i=0; i<5; i++) {
                            String id = " "+(count-update_window+1)+"-"+i;
                            String s = (String)_htree.get("key"+id);
                            if ((s == null) || !s.equals("value"+id)) {
                                throw new Error("Invalid value.  Expected: "
                                                +("value"+id)
                                                +", got: "
                                                +s);
                            }
                            _htree.put("key"+id, s+"-updated");
                        }
                    }
                }

                _htree.put("count", new Integer(count+1));
                _recman.commit();

                count++;
            }

            // BTW:  There is no cleanup.  It's a crash test after all.

        } catch (IOException ioe) {
            ioe.printStackTrace();
        }
    }

    void checkConsistency() {
        // TODO
    }

    public static void main(String[] args) {
        System.out.print("Please try to stop me anytime. ");
        System.out.println("CTRL-C, kill -9, anything goes!.");
        new CrashTest();
    }
}
