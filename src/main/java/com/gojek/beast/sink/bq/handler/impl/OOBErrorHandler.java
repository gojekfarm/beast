package com.gojek.beast.sink.bq.handler.impl;

import com.gojek.beast.models.Record;
import com.gojek.beast.models.Records;
import com.gojek.beast.models.Status;
import com.gojek.beast.sink.bq.handler.BQInsertionRecordsErrorType;
import com.gojek.beast.sink.bq.handler.BQErrorHandler;
import com.gojek.beast.sink.bq.handler.ErrorWriter;
import com.gojek.beast.sink.bq.handler.WriteStatus;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@AllArgsConstructor
@Slf4j
public class OOBErrorHandler implements BQErrorHandler {

    private final ErrorWriter errorWriter;

    @Override
    public Status handleErrorRecords(Map<Record, List<BQInsertionRecordsErrorType>> records) {
        if (records.isEmpty()) {
            return new WriteStatus(true, Optional.ofNullable(null));
        }
        boolean shouldBatchFail = false;
        final List<Record> recordsToWrite = new ArrayList<>();
        for (Record record : records.keySet()) {
            final List<BQInsertionRecordsErrorType> errorTypesForRecord = records.get(record);
            boolean recordHasOutOfBoundsData = false;
            boolean recordHasInvalidData = false;
            for (final BQInsertionRecordsErrorType errorType : errorTypesForRecord) {
                switch (errorType) {
                    case OOB: recordHasOutOfBoundsData = true;
                        break;
                    case INVALID: recordHasInvalidData = true;
                        break;
                    default:
                }
            } //end of for each row
            if (recordHasOutOfBoundsData) {
                //add the record into write list
                recordsToWrite.add(record);
            }
            if (recordHasInvalidData) {
                shouldBatchFail = true;
            }
        }
        if (shouldBatchFail) {
            //lets not store OOB records as well as the batch contains invalid/schema related errors
            log.info("Batch with records size: {} contains invalid records, marking this batch to fail", records.size());
            return new WriteStatus(false, Optional.ofNullable(null));
        }
        log.info("Error handler parsed OOB records size {}, handoff to the writer {}", recordsToWrite.size(), errorWriter.getClass().getSimpleName());
        final Status errorSinkStatus = errorWriter.writeErrorRecords(recordsToWrite);
        return new WriteStatus(errorSinkStatus.isSuccess() && !shouldBatchFail, Optional.ofNullable(null));
    }

    @Override
    public Records getBQValidRecords(Map<Record, List<BQInsertionRecordsErrorType>> records) {
        final List<Record> validRecords = new ArrayList<>();
        for (Record record : records.keySet()) {
            final List<BQInsertionRecordsErrorType> errorTypesForRecord = records.get(record);
            boolean recordHasOutOfBoundsData = false;
            boolean recordHasInvalidData = false;
            boolean recordHasValidData = false;
            for (final BQInsertionRecordsErrorType errorType : errorTypesForRecord) {
                switch (errorType) {
                    case OOB: recordHasOutOfBoundsData = true;
                        break;
                    case INVALID: recordHasInvalidData = true;
                        break;
                    case VALID: recordHasValidData = true;
                        break;
                    default:
                }
            } //end of for each row
            if (recordHasValidData
                    && !recordHasOutOfBoundsData
                    && !recordHasInvalidData) {
                //add the record into valid record list
                validRecords.add(record);
            }
        }
        return new Records(validRecords);
    }

}
