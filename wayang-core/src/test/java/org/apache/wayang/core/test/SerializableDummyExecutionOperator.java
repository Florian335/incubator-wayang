package org.apache.wayang.core.test;

import org.json.JSONObject;
import org.apache.wayang.core.plan.wayangplan.ExecutionOperator;
import org.apache.wayang.core.util.JsonSerializable;

/**
 * Dummy {@link ExecutionOperator} for test purposes.
 */
public class SerializableDummyExecutionOperator extends DummyExecutionOperator implements JsonSerializable {

    public SerializableDummyExecutionOperator(int someProperty) {
        super(1, 1, false);
        this.setSomeProperty(someProperty);
    }

    @Override
    public JSONObject toJson() {
        return new JSONObject().put("someProperty", this.getSomeProperty());
    }

    @SuppressWarnings("unused")
    public static SerializableDummyExecutionOperator fromJson(JSONObject jsonObject) {
        return new SerializableDummyExecutionOperator(jsonObject.getInt("someProperty"));
    }
}
