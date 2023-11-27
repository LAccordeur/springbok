package com.rogerguo.test.common;

/**
 * @author yangguo
 * @create 2021-12-29 11:50 AM
 **/
public class PortoTaxiPoint extends TrajectoryPoint {

    private String tripId;

    private String callType;

    private int originCall;

    private int originStand;

    //private String taxiId;  oid is taxi id

    private String dataType;

    private boolean missingData;


    public PortoTaxiPoint(String taxiId, long timestamp, double longitude, double latitude, String tripId, String callType, int originCall, int originStand,String dataType, boolean missingData) {
        super(taxiId, timestamp, longitude, latitude);
        this.tripId = tripId;
        this.callType = callType;
        this.originCall = originCall;
        this.originStand = originStand;
        this.dataType = dataType;
        this.missingData = missingData;

    }

    public String getTripId() {
        return tripId;
    }

    public void setTripId(String tripId) {
        this.tripId = tripId;
    }

    public String getCallType() {
        return callType;
    }

    public void setCallType(String callType) {
        this.callType = callType;
    }

    public int getOriginCall() {
        return originCall;
    }

    public void setOriginCall(int originCall) {
        this.originCall = originCall;
    }

    public int getOriginStand() {
        return originStand;
    }

    public void setOriginStand(int originStand) {
        this.originStand = originStand;
    }


    public String getDataType() {
        return dataType;
    }

    public void setDataType(String dataType) {
        this.dataType = dataType;
    }

    public boolean isMissingData() {
        return missingData;
    }

    public void setMissingData(boolean missingData) {
        this.missingData = missingData;
    }

    @Override
    public String toString() {
        return "PortoTaxiPoint{" +
                "tripId='" + tripId + '\'' +
                ", callType='" + callType + '\'' +
                ", originCall=" + originCall +
                ", originStand=" + originStand +
                ", dataType='" + dataType + '\'' +
                ", missingData=" + missingData +
                "} " + super.toString();
    }
}
