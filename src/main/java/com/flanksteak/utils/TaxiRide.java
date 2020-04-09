/*
 * Copyright 2018 data Artisans GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.flanksteak.utils;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.flink.api.common.typeinfo.Types;


import java.io.IOException;

/**
 * POJO representing a taxi ride.
 *
 * All fields are public for ease of use in our example.
 */
public class TaxiRide {

        // id of the taxi
    public String medallion;
    // license of the driver
    public String licenseId;
    // time when passengers were picked up
    public long pickUpTime;
    // time when passengers were dropped off
    public long dropOffTime;
    // longitude where passengers were picked up
    public float pickUpLon;
    // latitude where passengers were picked up
    public float pickUpLat;
    // longitude where passengers were dropped off
    public float dropOffLon;
    // latitude where passengers were dropped off
    public float dropOffLat;
    // total amount paid by the passengers
    public float total;

    //Empty Constructor
    public TaxiRide() {}


    public static TypeInformation[] inputFieldTypes = new TypeInformation[]{
            Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.INT, Types.FLOAT,
            Types.FLOAT, Types.FLOAT, Types.FLOAT, Types.FLOAT,
            Types.STRING, Types.FLOAT, Types.FLOAT, Types.FLOAT, Types.FLOAT, Types.FLOAT, Types.FLOAT
    };

    //Basic Constructor
    public TaxiRide(String medallion, String licenseId, long pickUpTime, long dropOffTime, float pickUpLon, float pickUpLat, float dropOffLon, float dropOffLat, float total) {

        this.medallion = medallion;
        this.licenseId = licenseId;
        this.pickUpTime = pickUpTime;
        this.dropOffLat = dropOffTime;
        this.pickUpLon = pickUpLon;
        this.pickUpLat = pickUpLat;
        this.dropOffLon = dropOffLon;
        this.dropOffLat = dropOffLat;
        this.total = total;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append('[');
        sb.append(medallion);
        sb.append(',');
        sb.append(licenseId);
        sb.append(',');
        sb.append(pickUpTime);
        sb.append(',');
        sb.append(dropOffTime);
        sb.append(',');
        sb.append(pickUpLon);
        sb.append(',');
        sb.append(pickUpLat);
        sb.append(',');
        sb.append(dropOffLon);
        sb.append(',');
        sb.append(dropOffLat);
        sb.append(',');
        sb.append(total);
        sb.append(']');
        return sb.toString();
    }

}

class TaxiRideDeserializationSchema implements
        DeserializationSchema<TaxiRide> {

    static ObjectMapper objectMapper = new ObjectMapper()
            .registerModule(new JavaTimeModule());

    @Override
    public TaxiRide deserialize(byte[] bytes) throws IOException {
        return objectMapper.readValue(bytes, TaxiRide.class);
    }

    @Override
    public boolean isEndOfStream(TaxiRide inputMessage) {
        return false;
    }

    @Override
    public TypeInformation<TaxiRide> getProducedType() {
        return TypeInformation.of(TaxiRide.class);
    }
}