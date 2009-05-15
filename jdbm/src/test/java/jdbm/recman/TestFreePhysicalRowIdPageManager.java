/*
 *  $Id: TestFreePhysicalRowIdPageManager.java,v 1.5 2005/08/25 00:21:05 boisvert Exp $
 *
 *  Unit tests for FreePhysicalRowIdPageManager class
 *
 *  Simple db toolkit
 *  Copyright (C) 1999, 2000 Cees de Groot <cg@cdegroot.com>
 *
 *  This library is free software; you can redistribute it and/or
 *  modify it under the terms of the GNU Library General Public License
 *  as published by the Free Software Foundation; either version 2
 *  of the License, or (at your option) any later version.
 *
 *  This library is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 *  Library General Public License for more details.
 *
 *  You should have received a copy of the GNU Library General Public License
 *  along with this library; if not, write to the Free Software Foundation,
 *  Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA
 */
package jdbm.recman;

import junit.framework.*;

/**
 *  This class contains all Unit tests for {@link FreePhysicalRowIdPageManager}.
 */
public class TestFreePhysicalRowIdPageManager extends TestCase {

    public TestFreePhysicalRowIdPageManager(String name) {
  super(name);
    }

    public void setUp() {
  TestRecordFile.deleteTestFile();
    }
    public void tearDown() {
  TestRecordFile.deleteTestFile();
    }

    /**
     *  Test constructor
     */
    public void testCtor() throws Exception {
  RecordFile f = new RecordFile(TestRecordFile.testFileName);
  PageManager pm = new PageManager(f);
      new FreePhysicalRowIdPageManager(f, pm);

      pm.close();
      f.close();
    }

    /**
     *  Test basics
     */
    public void testBasics() throws Exception {
  RecordFile f = new RecordFile(TestRecordFile.testFileName);
  PageManager pm = new PageManager(f);
  FreePhysicalRowIdPageManager freeMgr =
      new FreePhysicalRowIdPageManager(f, pm);

  // allocate 10,000 bytes - should fail on an empty file.
  Location loc = freeMgr.get(10000);
  assertTrue("loc is not null?", loc == null);

      pm.close();
      f.close();
    }


    /**
     *  Runs all tests in this class
     */
    public static void main(String[] args) {
  junit.textui.TestRunner.run(new TestSuite(TestFreePhysicalRowIdPageManager.class));
    }
}
