package org.usergrid.dashboard.aspects;

import org.usergrid.dashboard.service.DashboardService;
import java.lang.reflect.Method;
import java.util.HashSet;
import java.util.Set;

import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.reflect.MethodSignature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.usergrid.management.ApplicationInfo;
import org.usergrid.management.OrganizationInfo;
import org.usergrid.management.OrganizationOwnerInfo;
import org.usergrid.management.UserInfo;

public class DashboardManagementServiceAspect {
    private static enum TraceOperation {

        ADMIN("createAdminUser", "createAdminFrom",
        "createAdminFromPrexistingPassword"), APPLICATION(
        "createApplication"), ORGANIZATION("createOrganization"), ADMINORG(
        "createOwnerAndOrganization");
        private Set<String> methodsSet = new HashSet<String>();

        private TraceOperation(String... methods) {
            for (String method : methods) {
                methodsSet.add(method);
            }
        }

        void applyTrace(DashboardService dashboardService, Object returnValue) {
            switch (this) {
                case ADMIN:
                    dashboardService.adminUserCreated((UserInfo) returnValue);
                    break;
                case APPLICATION:
                    dashboardService
                            .applicationCreated((ApplicationInfo) returnValue);
                    break;
                case ORGANIZATION:
                    dashboardService
                            .organizationCreated((OrganizationInfo) returnValue);
                    break;
                case ADMINORG:
                    dashboardService
                            .organizationOwnerCreated((OrganizationOwnerInfo) returnValue);
                    break;
            }
        }

        boolean isAdviceFor(Method method) {
            return methodsSet.contains(method.getName());
        }
    }
    private static final Logger logger = LoggerFactory
            .getLogger(DashboardManagementServiceAspect.class);
    private DashboardService dashboardService;

    public Object applyTrace(ProceedingJoinPoint pjp) throws Throwable {
        Object returnObject = pjp.proceed();
        try {
            if (pjp.getSignature() instanceof MethodSignature) {
                MethodSignature methodSignature = (MethodSignature) pjp
                        .getSignature();
                for (TraceOperation traceOperation : TraceOperation.values()) {
                    if (traceOperation.isAdviceFor(methodSignature.getMethod())) {
                        logger.info("managementService trace for {}", pjp.toLongString());
                        traceOperation.applyTrace(getDashboardService(),
                                returnObject);
                    }
                }
            }
        } catch (Exception e) {
            logger.warn("dashboard trace cannot be invoked", e);
        }
        return returnObject;
    }

    public DashboardService getDashboardService() {
        return dashboardService;
    }

    public void setDashboardService(DashboardService dashboardService) {
        this.dashboardService = dashboardService;
    }
}
