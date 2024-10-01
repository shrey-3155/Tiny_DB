package org.example.Operations;

import java.io.IOException;

public interface TransactionOperationsInterface {
    boolean isTransactionStarted();

    void startTransaction() throws IOException;

    void executeBufferedOperations() throws IOException;

    void rollbackTransaction() throws IOException;

}
