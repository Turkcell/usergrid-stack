package org.usergrid.dashboard.service;

import java.util.List;
import org.usergrid.dashboard.domain.UsergridApplicationProperties;
import org.usergrid.dashboard.domain.UsergridCounter;
import org.usergrid.management.ApplicationInfo;
import org.usergrid.management.OrganizationInfo;
import org.usergrid.management.OrganizationOwnerInfo;
import org.usergrid.management.UserInfo;

public interface DashboardService {

    public static final String ADMINUSER_COUNTER = "numberOfAdminUsers";
    public static final String APPLICATIONS_COUNTER = "numberOfApplications";
    public static final String ORGANIZATIONS_COUNTER = "numberOfOrganizations";

    void applicationCreated(ApplicationInfo applicationInfo);

    void organizationCreated(OrganizationInfo organizationInfo);

    void organizationOwnerCreated(OrganizationOwnerInfo organizationOwnerInfo);

    void adminUserCreated(UserInfo userInfo);

    void appUserCreated(ApplicationInfo applicationInfo);

    List<UsergridApplicationProperties> getApplicationProperties();

    List<UsergridCounter> getDashboardCounters();

    List<UsergridApplicationProperties> getDashboardCountersOrderByCount(Integer start, Integer count);
}
