package org.usergrid.dashboard;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import org.usergrid.dashboard.service.DashboardService;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import org.junit.AfterClass;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.usergrid.dashboard.domain.UsergridCounter;
import org.usergrid.management.ManagementService;
import org.usergrid.management.ManagementTestHelper;
import org.usergrid.management.OrganizationOwnerInfo;
import org.usergrid.management.cassandra.ManagementTestHelperImpl;
import org.usergrid.persistence.Entity;
import org.usergrid.persistence.EntityManager;
import org.usergrid.persistence.TypedEntity;
import org.usergrid.persistence.cassandra.EntityManagerFactoryImpl;
import org.usergrid.persistence.entities.User;
import org.usergrid.services.ServiceManager;
import org.usergrid.services.ServiceManagerFactory;

public class DashboardServiceTest {

    private static final Logger logger = LoggerFactory
            .getLogger(DashboardServiceTest.class);
    static ManagementTestHelper helper;
    static ManagementService management;
    static DashboardService dashboardService;
    private final EntityManagerFactoryImpl emf;
    static ServiceManagerFactory smf=null;
    @Autowired
    protected Properties properties;

    public DashboardServiceTest() {
        this.emf = (EntityManagerFactoryImpl) helper.getEntityManagerFactory();
    }

    @BeforeClass
    public static void setup() throws Exception {
        logger.info("setup");
        assertNull(helper);
        helper = new ManagementTestHelperImpl();
        // helper.setClient(this);
        helper.setup();
        management = helper.getManagementService();
        smf=helper.getApplicationContext().getBean(ServiceManagerFactory.class);
        dashboardService = helper.getApplicationContext().getBean(
                DashboardService.class);
    }

    @AfterClass
    public static void teardown() throws Exception {
        logger.info("teardown");
        helper.teardown();
    }
    private static int uniqueCounter;

    private String getUniqueEmail() {
        return getUniqueString() + "@" + getUniqueString() + ".com";
    }

    private String getUniqueString() {
        uniqueCounter++;
        return "dashboardtest" + uniqueCounter;
    }

    private long getDashboardCounter(String name) {
        List<UsergridCounter> dashboardCounters = dashboardService.getDashboardCounters();
        for (UsergridCounter usergridCounter : dashboardCounters) {
            if (usergridCounter.getName().equals(name)) {
                return usergridCounter.getCounter();
            }
        }
        return 0L;
    }

    @Test
    public void countAdminUserTest() throws Exception {
        Long initialAdminUserCount = getDashboardCounter(DashboardService.ADMINUSER_COUNTER);
        management.createAdminUser(getUniqueString(), getUniqueString(),
                getUniqueEmail(), getUniqueString(), true, false);
        assertEquals(1 + initialAdminUserCount,
                getDashboardCounter(DashboardService.ADMINUSER_COUNTER));

    }

    @Test
    public void countAdminUserFailTest() throws Exception {
        Long initialAdminUserCount = getDashboardCounter(DashboardService.ADMINUSER_COUNTER);
        String userName = getUniqueString();
        try {
            management.createAdminUser(userName, getUniqueString(),
                    getUniqueEmail(), getUniqueString(), true, false);
            management.createAdminUser(userName, getUniqueString(),
                    getUniqueEmail(), getUniqueString(), true, false);
        } catch (Exception e) {
        }
        assertEquals(1 + initialAdminUserCount,
                getDashboardCounter(DashboardService.ADMINUSER_COUNTER));
    }

    @Test
    public void countApplicationTest() throws Exception {
        long initialApplicationUserCount = getDashboardCounter(DashboardService.APPLICATIONS_COUNTER);
        OrganizationOwnerInfo organization = management
                .createOwnerAndOrganization(getUniqueString(),
                getUniqueString(), getUniqueString(), getUniqueEmail(),
                getUniqueString());

        management.createApplication(organization.getOrganization().getUuid(),
                getUniqueString());

        assertEquals(1 + initialApplicationUserCount,
                getDashboardCounter(DashboardService.APPLICATIONS_COUNTER));
    }

    @Test
    public void countApplicationFailTest() throws Exception {
        long initialApplicationUserCount = getDashboardCounter(DashboardService.APPLICATIONS_COUNTER);
        OrganizationOwnerInfo organization = management
                .createOwnerAndOrganization(getUniqueString(),
                getUniqueString(), getUniqueString(), getUniqueEmail(),
                getUniqueString());

        String appName = getUniqueString();
        try {
            management.createApplication(organization.getOrganization().getUuid(),
                    appName);
            management.createApplication(organization.getOrganization().getUuid(),
                    appName);
        } catch (Exception e) {
        }

        assertEquals(1 + initialApplicationUserCount,
                getDashboardCounter(DashboardService.APPLICATIONS_COUNTER));
    }

    @Test
    public void countOrganizationTest() throws Exception {
        long initialOrganizationCount = getDashboardCounter(DashboardService.ORGANIZATIONS_COUNTER);
        String uniqueString = getUniqueString();

        management.createOwnerAndOrganization(uniqueString, getUniqueString(),
                getUniqueString(), getUniqueEmail(), getUniqueString());
        assertEquals(1 + initialOrganizationCount,
                getDashboardCounter(DashboardService.ORGANIZATIONS_COUNTER));
    }

    @Test
    public void countOrganizationFailTest() throws Exception {
        long initialOrganizationCount = getDashboardCounter(DashboardService.ORGANIZATIONS_COUNTER);
        String uniqueString = getUniqueString();
        try {
            management.createOwnerAndOrganization(uniqueString, getUniqueString(),
                    getUniqueString(), getUniqueEmail(), getUniqueString());
            management.createOwnerAndOrganization(uniqueString, getUniqueString(),
                    getUniqueString(), getUniqueEmail(), getUniqueString());
        } catch (Exception e) {
        }
        assertEquals(1 + initialOrganizationCount,
                getDashboardCounter(DashboardService.ORGANIZATIONS_COUNTER));
    }

    @Test
    public void testUserAssign() {
        assertTrue(TypedEntity.class.isAssignableFrom(User.class));
    }
    
    @Test
    public void testRecreateCounters(){
        dashboardService.resetCounters();
        assertFalse(dashboardService.getDashboardCounters().isEmpty());
    }
    @Test
    public void testUserCreate() throws Exception {
        UUID applicationId = emf.createApplication("testOrganization",
                "testPermissions");
        assertNotNull(applicationId);

        ServiceManager sm = smf.getServiceManager(applicationId);
        assertNotNull(sm);

        EntityManager em = sm.getEntityManager();
        assertNotNull(em);

        Map<String, Object> properties = new LinkedHashMap<String, Object>();
        properties.put("username", "edanuff");
        properties.put("email", "ed@anuff.com");

        int initialSize = dashboardService.getApplicationProperties().size();
        Entity user = em.create("user", properties);
        assertNotNull(user);
        assertEquals(initialSize + 1, dashboardService.getApplicationProperties().size());
    }
}
