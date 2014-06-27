package com.mongodb.flac;

/**
 * RedactedDBCollection Constants.
 */
public class RedactedDBCollectionConstants {
    private static String securityExpression;

    public static String getSecurityExpression() {
        if (securityExpression == null) { securityExpression = getDefaultSecurityExpression(); }
        return securityExpression;
    }


    public void setSecurityExpression(String securityExpression) {          // non static so users can use spring easily
        RedactedDBCollectionConstants.securityExpression = securityExpression;
    }

    private static String getDefaultSecurityExpression() {
        return "{\n" +
                "        $cond: {\n" +
                "            if: {\n" +
                "                $allElementsTrue : {\n" +
                "                    $map : {\n" +
                "                        input: {$ifNull:[\"$sl\",[[]]]},\n" +
                "                        \"as\" : \"setNeeded\",\n" +
                "                            \"in\" : {\n" +
                "                            $cond: {\n" +
                "                                if: {\n" +
                "                                    $or: [\n" +
                "                                        { $eq: [ { $size: \"$$setNeeded\" }, 0 ] },\n" +
                "                                        { $gt: [ { $size: { $setIntersection: [ \"$$setNeeded\", %s ] } }, 0 ] }\n" +
                "                                    ]\n" +
                "                                },\n" +
                "                                then: true,\n" +
                "                            else: false\n" +
                "                            }\n" +
                "                        }\n" +
                "                    }\n" +
                "                }\n" +
                "            },\n" +
                "            then: \"$$DESCEND\",\n" +
                "        else: \"$$PRUNE\"\n" +
                "        }\n" +
                "}\n";
    }
}
