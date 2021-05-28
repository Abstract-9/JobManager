package com.jamz.jobManager.util;

public class Location {
    public double latitude, longitude, altitude;

    public Location(double latitude, double longitude, double altitude) {
        this.latitude = latitude;
        this.longitude = longitude;
        this.altitude = altitude;
    }

    /**
     * Gets the ground distance in metres between two LatLon objects.
     *
     * This method is way better than what we were using previously.
     * It uses the 'haversine' formula.
     *
     * @param other the other point to measure distance to
     * @return the distance from this point to the other point
     */
    public double getDistanceTo(Location other) {
        double lat1 = this.latitude, lat2 = other.latitude;
        double lon1 = this.longitude, lon2 = other.longitude;

        double r = 6371e3; // radius, in metres
        double phi1 = lat1 * Math.PI/180; // φ, λ in radians
        double phi2 = lat2 * Math.PI/180;
        double deltaPhi = (lat2-lat1) * Math.PI/180;
        double deltaLambda = (lon2-lon1) * Math.PI/180;

        double a = Math.sin(deltaPhi/2) * Math.sin(deltaPhi/2) +
                        Math.cos(phi1) * Math.cos(phi2) *
                                Math.sin(deltaLambda/2) * Math.sin(deltaLambda/2);
        double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1-a));

        return r * c; // in metres
    }
}
