package com.rogerguo.test.index.predicate;

import com.rogerguo.test.index.predicate.BasicQueryPredicate;

/**
 * @author yangguo
 * @create 2021-06-24 5:11 PM
 **/
public class IdTemporalQueryPredicate extends BasicQueryPredicate {

    private String deviceId;

    public IdTemporalQueryPredicate() {}

    public IdTemporalQueryPredicate(long startTimestamp, long stopTimestamp, String deviceId) {
        super(startTimestamp, stopTimestamp);
        this.deviceId = deviceId;
    }

    public String getDeviceId() {
        return deviceId;
    }

    public void setDeviceId(String deviceId) {
        this.deviceId = deviceId;
    }

    @Override
    public String toString() {
        return "IdTemporalQueryPredicate{" +
                "deviceId='" + deviceId + '\'' +
                "} " + super.toString();
    }
}
