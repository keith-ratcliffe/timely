package qonduit.operations.version;

import qonduit.operations.OperationRequest;

public class VersionRequest extends OperationRequest {

    public static final String operation = "version";

    public String getOperation() {
        return operation;
    }

    @Override
    public String toString() {
        return "{ \"operation\": \"version\" }";
    }

}
