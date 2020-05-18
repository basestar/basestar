package io.basestar.database.options;

import io.basestar.expression.Expression;
import lombok.Builder;
import lombok.Data;

import java.util.Map;

/*
SOCIETY
{
    "group": {
        "ipsociety": "nameofrecipient"
    },
    "aggregate": {
        "royalties": "round(sum(ipRemittedRoyaltyAmount), 2)"
    }
}

RECORDING
{
    "filter": "isrc != null",
    "group": {
        "isrc": "isrc"
    },
    "aggregate": {
        "originalworktitle": "max(originalworktitle)",
        "royalties": "round(sum(ipRemittedRoyaltyAmount), 2)"
    }
}

PUBLISHER
{
    "filter": "generictypeofip == 'P'",
    "group": {
        "interestedpartynumber": "ipname"
    },
    "aggregate": {
        "royalties": "round(sum(ipRemittedRoyaltyAmount), 2)"
    }
}

WORK
{
    "filter": "generictypeofip == 'P'",
    "group": {
        "societyworkidentifier": "societyworkidentifier"
    },
    "aggregate": {
        "originalworktitle": "max(originalworktitle)",
        "royalties": "round(sum(ipRemittedRoyaltyAmount), 2)"
    }
}

DSP
{
    "filter": "generictypeofip == 'C'",
    "group": {
        "exploitationsourcename": "exploitationsourcename"
    },
    "aggregate": {
        "royalties": "round(sum(ipRemittedRoyaltyAmount), 2)"
    }
}
 */

@Data
@Builder(toBuilder = true, builderClassName = "Builder")
public class AggregateOptions {

    public static final String TYPE = "aggregate";

    private final String schema;

    private final Expression expression;

    private final Map<String, Expression> group;

    private final Map<String, Expression> aggregate;
}
