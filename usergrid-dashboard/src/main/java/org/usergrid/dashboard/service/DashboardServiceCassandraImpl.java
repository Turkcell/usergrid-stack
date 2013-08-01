/*
 * Copyright 2013 Turkcell Teknoloji Inc. and individual
 * contributors by the @authors tag.
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
package org.usergrid.dashboard.service;

import com.usergrid.count.CassandraCounterStore;
import com.usergrid.count.CounterStore;
import com.usergrid.count.common.Count;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.CounterSlice;
import me.prettyprint.hector.api.beans.HCounterColumn;
import me.prettyprint.hector.api.ddl.ColumnFamilyDefinition;
import me.prettyprint.hector.api.ddl.ColumnType;
import me.prettyprint.hector.api.ddl.ComparatorType;
import static me.prettyprint.hector.api.factory.HFactory.createColumnFamilyDefinition;
import static me.prettyprint.hector.api.factory.HFactory.createCounterSliceQuery;
import me.prettyprint.hector.api.query.SliceCounterQuery;
import org.usergrid.dashboard.domain.UsergridApplicationProperties;
import org.usergrid.dashboard.domain.UsergridCounter;
import org.usergrid.management.ApplicationInfo;
import org.usergrid.management.OrganizationInfo;
import org.usergrid.management.OrganizationOwnerInfo;
import org.usergrid.management.UserInfo;
import org.usergrid.persistence.cassandra.CassandraService;

/**
 *
 * @author capacman
 */
public class DashboardServiceCassandraImpl implements DashboardService {

    private static final String DASHBOARD_KEYSPACE = "Usergrid_Dashboard";
    public static final String SYSTEM_KEY = "System";
    private static final String APPLICATIONS_KEY = "Applications";
    public static final String DASHBOARD_COUNTERS_CF = "Dashboard_Counters";
    private CassandraService cass;
    private CounterStore counterStore;
    private Keyspace keyspace;

    public void setCass(CassandraService cass) {
        this.cass = cass;
    }

    public void init() {
        ColumnFamilyDefinition cfDef = createColumnFamilyDefinition(DASHBOARD_KEYSPACE, DASHBOARD_COUNTERS_CF, ComparatorType.COUNTERTYPE);
        cfDef.setKeyValidationClass(ComparatorType.UTF8TYPE.getClassName());
        cfDef.setComparatorType(ComparatorType.UTF8TYPE);
        cfDef.setDefaultValidationClass(ComparatorType.COUNTERTYPE.getClassName());
        cfDef.setColumnType(ColumnType.STANDARD);
        cass.createColumnFamily(DASHBOARD_KEYSPACE, cfDef);
        keyspace = cass.getKeyspace(DASHBOARD_KEYSPACE, null);
        counterStore = new CassandraCounterStore(keyspace);
    }

    @Override
    public void applicationCreated(ApplicationInfo applicationInfo) {
        incrementCounter(SYSTEM_KEY, APPLICATIONS_COUNTER);
    }

    @Override
    public void organizationCreated(OrganizationInfo organizationInfo) {
        incrementCounter(SYSTEM_KEY, ORGANIZATIONS_COUNTER);
    }

    @Override
    public void organizationOwnerCreated(OrganizationOwnerInfo organizationOwnerInfo) {
        counterStore.save(Arrays.asList(createCount(SYSTEM_KEY, ORGANIZATIONS_COUNTER), createCount(SYSTEM_KEY, ADMINUSER_COUNTER)));
    }

    @Override
    public void adminUserCreated(UserInfo userInfo) {
        incrementCounter(SYSTEM_KEY, ADMINUSER_COUNTER);
    }

    @Override
    public void appUserCreated(ApplicationInfo applicationInfo) {
        incrementCounter(APPLICATIONS_KEY, applicationInfo.getName() + ";" + applicationInfo.getId());
    }

    @Override
    public List<UsergridCounter> getDashboardCounters() {

        SliceCounterQuery<String, String> sliceQuery = createCounterSliceQuery(keyspace, StringSerializer.get(), StringSerializer.get());
        sliceQuery.setColumnFamily(DASHBOARD_COUNTERS_CF);
        sliceQuery.setKey(SYSTEM_KEY);
        sliceQuery.setColumnNames(ADMINUSER_COUNTER, APPLICATIONS_COUNTER, ORGANIZATIONS_COUNTER);
        CounterSlice<String> result = sliceQuery.execute().get();
        List<UsergridCounter> counters = new ArrayList<UsergridCounter>(3);
        for (HCounterColumn<String> column : result.getColumns()) {
            counters.add(new UsergridCounter(column.getName(), column.getValue()));
        }
        return counters;
    }

    @Override
    public List<UsergridApplicationProperties> getDashboardCountersOrderByCount(Integer start, Integer count) {
        List<UsergridApplicationProperties> applicationProperties = getApplicationProperties();
        if (applicationProperties.size() <= start) {
            return Collections.EMPTY_LIST;
        }
        return applicationProperties.subList(start, applicationProperties.size() > start + count ? start + count : applicationProperties.size() - 1);
    }

    private void incrementCounter(String key, String counter) {
        counterStore.save(createCount(key, counter));
    }

    private Count createCount(String key, String counter) {
        return new Count(DASHBOARD_COUNTERS_CF, key, counter, 1);
    }

    @Override
    public List<UsergridApplicationProperties> getApplicationProperties() {
        SliceCounterQuery<String, String> sliceQuery = createCounterSliceQuery(keyspace, StringSerializer.get(), StringSerializer.get());
        sliceQuery.setColumnFamily(DASHBOARD_COUNTERS_CF);
        sliceQuery.setKey(APPLICATIONS_KEY);
        sliceQuery.setRange(null, null, false, 10000);
        CounterSlice<String> result = sliceQuery.execute().get();
        List<UsergridApplicationProperties> counters = new ArrayList<UsergridApplicationProperties>(3);
        for (HCounterColumn<String> column : result.getColumns()) {
            final String columnName = column.getName();
            final int seperator = columnName.lastIndexOf(";");
            counters.add(new UsergridApplicationProperties(columnName.substring(seperator), columnName.substring(0, seperator), column.getValue()));
        }
        Collections.sort(counters);
        return counters;
    }

}
