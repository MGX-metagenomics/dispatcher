package de.cebitec.mgx.dispatcher;

import de.cebitec.mgx.dispatcher.common.MGXDispatcherException;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author sjaenick
 */
public class FactoryHolderTest {

    public FactoryHolderTest() {
    }

    @BeforeClass
    public static void setUpClass() {
    }

    @AfterClass
    public static void tearDownClass() {
    }

    @Before
    public void setUp() {
    }

    @After
    public void tearDown() {
    }

    @Test
    public void testGetFactory() {
        System.out.println("getFactory");
        String projClass = "doesnotexist";
        FactoryHolder fh = new FactoryHolder();
        try {
            fh.getFactory(projClass);
        } catch (MGXDispatcherException ex) {
            return;
        }
        fail();
    }

    @Test
    public void testRegisterFactory() {
        System.out.println("registerFactory");
        String projClass = "";
        JobFactoryI fact = null;
        FactoryHolder fh = new FactoryHolder();
        try {
            fh.registerFactory(projClass, fact);
        } catch (MGXDispatcherException ex) {
            return;
        }
        fail();
    }

    @Test
    public void testRegisterUnregisterFactory() {
        System.out.println("registerUnregisterFactory");
        String projClass = "TEST";
        JobFactoryI fact = new JobFactoryI() {

            @Override
            public JobI createJob(String projName, long jobId) {
                return null;
            }
        };
        FactoryHolder fh = new FactoryHolder();
        try {
            fh.registerFactory(projClass, fact);
        } catch (MGXDispatcherException ex) {
            fail(ex.getMessage());
        }
        JobFactoryI uf = fh.unregisterFactory(projClass);
        assertEquals(uf, fact);
        uf = fh.unregisterFactory(projClass);
        assertNull(uf);
    }

    @Test
    public void testRegisterNullPClass() {
        System.out.println("registerNullPClass");
        String projClass = null;
        JobFactoryI fact = null;
        FactoryHolder fh = new FactoryHolder();
        try {
            fh.registerFactory(projClass, fact);
        } catch (MGXDispatcherException ex) {
            return;
        }
        fail("Erroneous registration of NULL projectClass");
    }

    @Test
    public void testRegisterNullFactory() {
        System.out.println("registerNullFactory");
        String projClass = "FOOO";
        FactoryHolder fh = new FactoryHolder();
        try {
            fh.registerFactory(projClass, null);
        } catch (MGXDispatcherException ex) {
            return;
        }
        fail("Erroneous registration of NULL FactoryI");
    }

    @Test
    public void testUnregisterFactory() {
        System.out.println("unregisterFactory");
        FactoryHolder fh = new FactoryHolder();
        JobFactoryI jf = fh.unregisterFactory("unregistered");
        assertNull(jf);
    }

}
