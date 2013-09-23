package org.usergrid.dashboard.domain;

import java.io.Serializable;

public class UsergridCounter implements Serializable {

    private String name;
    private long counter;

    public UsergridCounter() {
    }

    public UsergridCounter(String name, long counter) {
        this.name = name;
        this.counter = counter;
    }

   
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public long getCounter() {
        return counter;
    }

    public void setCounter(long counter) {
        this.counter = counter;
    }

    @Override
    public int hashCode() {
        int hash = 7;
        hash = 59 * hash + (this.name != null ? this.name.hashCode() : 0);
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final UsergridCounter other = (UsergridCounter) obj;
        if ((this.name == null) ? (other.name != null) : !this.name.equals(other.name)) {
            return false;
        }
        return true;
    }
    
    @Override
    public String toString() {
        return "UsergridCounter{" + "name=" + name + ", counter=" + counter + '}';
    }

}
