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
package org.usergrid.dashboard.domain;

import java.io.Serializable;

/**
 *
 * @author capacman
 */
public class UsergridApplicationProperties implements Serializable, Comparable<UsergridApplicationProperties> {

    private static final long serialVersionUID = 1L;
    private String uuid;
    private String name;
    private Long userCount;

    public UsergridApplicationProperties() {
    }

    public UsergridApplicationProperties(String uuid, String name, Long userCount) {
        this.uuid = uuid;
        this.name = name;
        this.userCount = userCount;
    }

    public Long getUserCount() {
        return userCount;
    }

    public void setUserCount(Long userCount) {
        this.userCount = userCount;
    }

    public String getUuid() {
        return uuid;
    }

    public void setUuid(String uuid) {
        this.uuid = uuid;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @Override
    public int compareTo(UsergridApplicationProperties o) {
        if (this.userCount < o.userCount) {
            return -1;
        } else if (this.userCount == o.userCount) {
            return 0;
        } else {
            return 1;
        }
    }
}
