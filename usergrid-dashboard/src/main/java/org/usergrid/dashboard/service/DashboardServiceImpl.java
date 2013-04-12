package org.usergrid.dashboard.service;

import java.util.List;
import javax.persistence.EntityManager;
import javax.persistence.Query;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataIntegrityViolationException;
import org.usergrid.dashboard.domain.UsergridApplicationProperties;
import org.usergrid.dashboard.domain.UsergridCounter;
import org.usergrid.management.ApplicationInfo;
import org.usergrid.management.OrganizationInfo;
import org.usergrid.management.OrganizationOwnerInfo;
import org.usergrid.management.UserInfo;

public class DashboardServiceImpl implements DashboardService {

    private static final Logger logger = LoggerFactory
            .getLogger(DashboardServiceImpl.class);
    private EntityManager em;

    public void setEntityManager(EntityManager em) {
        this.em = em;
    }

    @Override
    public void applicationCreated(ApplicationInfo applicationInfo) {
        logger.info("application {} created", applicationInfo.getName());
        checkAndUpdate(APPLICATIONS_COUNTER);
    }

    @Override
    public void organizationCreated(OrganizationInfo organizationInfo) {
        logger.info("organization {} created", organizationInfo.getName());
        checkAndUpdate(ORGANIZATIONS_COUNTER);
    }

    @Override
    public void organizationOwnerCreated(
            OrganizationOwnerInfo organizationOwnerInfo) {
        logger.info("organization {} and admin {} created", organizationOwnerInfo.getOrganization().getName(), organizationOwnerInfo.getOwner().getName());
        checkAndUpdate(ADMINUSER_COUNTER);
        checkAndUpdate(ORGANIZATIONS_COUNTER);
    }

    @Override
    public void adminUserCreated(UserInfo userInfo) {
        logger.info("adminUser {} created", userInfo.getName());
        checkAndUpdate(ADMINUSER_COUNTER);
    }

    @Override
    public List<UsergridCounter> getDashboardCounters() {
        Query query = em.createQuery("select c From UsergridCounter c");
        return query.getResultList();
    }

    @Autowired
    public void setEntityManagerFactory(EntityManager em) {
        this.em = em;
    }

    @Override
    public void appUserCreated(ApplicationInfo applicationInfo) {
        logger.info("user created for application {} ", applicationInfo.getName());
        int count = updateApplicationCounter(applicationInfo);
        if (count <= 0) {
            UsergridApplicationProperties uap = new UsergridApplicationProperties();
            uap.setName(applicationInfo.getName());
            uap.setUserCount(0L);
            uap.setUuid(applicationInfo.getId().toString());
            try {
                em.persist(uap);
            } catch (DataIntegrityViolationException e) {
                logger.warn("appProperty for {} already created", applicationInfo.getId());
            }
            updateApplicationCounter(applicationInfo);
        }
    }

    private void checkAndUpdate(String counterName) {
        int counter = prepareQuery(counterName).executeUpdate();
        if (counter <= 0) {
            createCounter(counterName);
            prepareQuery(counterName).executeUpdate();
        }
    }

    private Query prepareQuery(String counterName) {
        return em.createQuery("UPDATE UsergridCounter c SET c.counter=c.counter+1 WHERE c.name='" + counterName + "'");
    }

    private void createCounter(String counterName) {
        UsergridCounter counter = new UsergridCounter();
        counter.setCounter(0);
        counter.setName(counterName);
        try {
            em.persist(counter);
        } catch (DataIntegrityViolationException e) {
            logger.warn("counter {} already created", counterName);
        }
    }

    private int updateApplicationCounter(ApplicationInfo applicationInfo) {
        Query query = em.createQuery("UPDATE UsergridApplicationProperties c SET c.userCount=c.userCount+1 WHERE c.uuid=:uuid");
        query.setParameter("uuid", applicationInfo.getId().toString());
        int count = query.executeUpdate();
        return count;
    }

    @Override
    public List<UsergridApplicationProperties> getApplicationProperties() {
        Query query = em.createQuery("SELECT ap FROM UsergridApplicationProperties ap ");
        return query.getResultList();
    }

    @Override
    public List<UsergridApplicationProperties> getDashboardCountersOrderByCount(Integer start, Integer end) {
        Query query = em.createQuery("select ap from UsergridApplicationProperties ap ORDER BY ap.userCount");
        if (start != null) {
            query.setFirstResult(start);
            if (end != null && end > start) {
                query.setMaxResults(end);
            }
        }
        return query.getResultList();
    }
}
