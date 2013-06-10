package org.usergrid.dashboard.domain;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DashboardCounters {
	private static final Logger logger = LoggerFactory
			.getLogger(DashboardCounters.class);
	private final long organizationCount;
	private final long applicationCount;
	private final long adminUserCount;

	public DashboardCounters(Long organizationCount, Long applicationCount,
			Long adminUserCount) {
		super();
		this.organizationCount = organizationCount != null ? organizationCount
				: 0;
		this.applicationCount = applicationCount != null ? applicationCount : 0;
		this.adminUserCount = adminUserCount != null ? adminUserCount : 0;
		logger.info(this.toString());
	}

	public long getOrganizationCount() {
		return organizationCount;
	}

	public long getApplicationCount() {
		return applicationCount;
	}

	public long getAdminUserCount() {
		return adminUserCount;
	}

	@Override
	public String toString() {
		return "DashboardCounters [organizationCount=" + organizationCount
				+ ", applicationCount=" + applicationCount
				+ ", adminUserCount=" + adminUserCount + "]";
	}
}
