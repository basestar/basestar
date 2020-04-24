package io.basestar.spark.aws;

import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;

public class AWSSparkUtils {

    public static Region defaultRegion() {

        final Region region = Regions.getCurrentRegion();
        if(region == null) {
            return Region.getRegion(Regions.DEFAULT_REGION);
        } else {
            return region;
        }
    }
}
