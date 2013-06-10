/*
 * Copyright 2013 capacman.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.usergrid.dashboard.web;

import java.util.List;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;
import org.usergrid.dashboard.domain.UsergridApplicationProperties;
import org.usergrid.dashboard.domain.UsergridCounter;
import org.usergrid.dashboard.service.DashboardService;

/**
 * 
 * @author capacman
 */
@Path("/dashboard")
@Component
@Scope("singleton")
@Produces({ MediaType.APPLICATION_JSON, "application/javascript",
		"application/x-javascript", "text/ecmascript",
		"application/ecmascript", "text/jscript" })
public class DashboardResource {

	private DashboardService dashboardService;

	public DashboardResource() {
		(new Exception()).printStackTrace();
	}

	@GET
	@Path("counters")
	public List<UsergridCounter> getDashboardCounters() {
		return dashboardService.getDashboardCounters();
	}

	@GET
	@Path("appsProperties")
	public List<UsergridApplicationProperties> getApplicationProperties(
			@QueryParam("start") Integer start, @QueryParam("start") Integer end) {
		return dashboardService.getDashboardCountersOrderByCount(start, end);
	}

	@Autowired
	public void setDashboardService(DashboardService dashboardService) {
		this.dashboardService = dashboardService;
	}
}
