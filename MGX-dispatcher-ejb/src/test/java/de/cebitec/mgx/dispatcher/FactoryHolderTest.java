package de.cebitec.mgx.dispatcher;

import de.cebitec.mgx.dispatcher.api.DispatcherI;
import de.cebitec.mgx.dispatcher.api.JobI;
import de.cebitec.mgx.dispatcher.api.JobFactoryI;
import de.cebitec.mgx.dispatcher.common.api.MGXDispatcherException;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.fail;
import org.junit.jupiter.api.Test;

/**
 *
 * @author sjaenick
 */
//@RunWithApplicationComposer
public class FactoryHolderTest {

    public FactoryHolderTest() {
    }

//    @Test
//    public void testGetFactory() {
//        System.out.println("getFactory");
//        String projClass = "doesnotexist";
//        FactoryHolder fh = new FactoryHolder();
//        try {
//            JobFactoryI factory = fh.getFactory(projClass);
//            assertNull(factory);
//        } catch (MGXDispatcherException ex) {
//            return;
//        }
//        fail();
//    }

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
            public JobI createJob(DispatcherI d, String projName, long jobId) {
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
