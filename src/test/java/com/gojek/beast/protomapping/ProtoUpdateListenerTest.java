package com.gojek.beast.protomapping;

import com.gojek.beast.config.AppConfig;
import com.gojek.beast.config.ColumnMapping;
import com.gojek.beast.config.ProtoMappingConfig;
import com.gojek.beast.models.ExternalCallException;
import com.gojek.beast.models.UpdateBQTableRequest;
import org.aeonbits.owner.ConfigFactory;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class ProtoUpdateListenerTest {
    @Captor
    private ArgumentCaptor<UpdateBQTableRequest> updateBQTableRequestArgumentCaptor;
    @Mock
    private UpdateTableService updateTableService;
    private ProtoUpdateListener protoUpdateListener;
    private ProtoMappingConfig protoMappingConfig;
    private AppConfig appConfig;


    @Before
    public void setUp() throws ExternalCallException {
        System.setProperty("ENABLE_AUTO_SCHEMA_UPDATE", "true");
        System.setProperty("PROTO_COLUMN_MAPPING_URL", "proto-mapping-url");
        System.setProperty("UPDATE_TABLE_SCHEMA_URL", "update-table-schema-url");
        appConfig = ConfigFactory.create(AppConfig.class, System.getProperties());
        protoMappingConfig = ConfigFactory.create(ProtoMappingConfig.class, System.getProperties());
        protoUpdateListener = new ProtoUpdateListener(protoMappingConfig, appConfig, updateTableService);
    }

    @Test
    public void shouldUseNewSchemaIfProtoChanges() throws ExternalCallException {
        String newProtoMapping = "{\"1\":\"test-1\",\"2\":\"test-2\"}";
        when(updateTableService.getProtoMappingFromRemoteURL(protoMappingConfig.getProtoColumnMappingURL(), appConfig.getProtoSchema())).thenReturn(newProtoMapping);
        when(updateTableService.updateBigQuerySchema(eq(protoMappingConfig.getUpdateBQTableURL()), updateBQTableRequestArgumentCaptor.capture())).thenReturn("success");

        protoUpdateListener.onProtoUpdate();

        verify(updateTableService, Mockito.times(3)).getProtoMappingFromRemoteURL(protoMappingConfig.getProtoColumnMappingURL(), appConfig.getProtoSchema());
        verify(updateTableService).updateBigQuerySchema(eq(protoMappingConfig.getUpdateBQTableURL()), eq(updateBQTableRequestArgumentCaptor.getValue()));
        ColumnMapping actualNewProtoMapping = protoMappingConfig.getProtoColumnMapping();
        Assert.assertEquals("test-1", actualNewProtoMapping.getProperty("1"));
        Assert.assertEquals("test-2", actualNewProtoMapping.getProperty("2"));
    }

    @Test
    public void shouldUseOldSchemaIfFetchUpdatedSchemaFails() throws ExternalCallException {
        ColumnMapping expectedProtoMapping = protoMappingConfig.getProtoColumnMapping();
        when(updateTableService.getProtoMappingFromRemoteURL(protoMappingConfig.getProtoColumnMappingURL(), appConfig.getProtoSchema())).thenThrow(new ExternalCallException("get updated mapping error"));

        protoUpdateListener.onProtoUpdate();
        ColumnMapping actualNewProtoMapping = protoMappingConfig.getProtoColumnMapping();
        Assert.assertEquals(expectedProtoMapping, actualNewProtoMapping);
    }

    @Test
    public void shouldUseOldSchemaIfUpdateSchemaFails() throws ExternalCallException {
        ColumnMapping expectedProtoMapping = protoMappingConfig.getProtoColumnMapping();
        String newProtoMapping = "{\"1\":\"test-1\",\"2\":\"test-2\"}";
        when(updateTableService.getProtoMappingFromRemoteURL(protoMappingConfig.getProtoColumnMappingURL(), appConfig.getProtoSchema())).thenReturn(newProtoMapping);
        when(updateTableService.updateBigQuerySchema(eq(protoMappingConfig.getUpdateBQTableURL()), updateBQTableRequestArgumentCaptor.capture())).thenThrow(new ExternalCallException("update bq table call exception"));

        protoUpdateListener.onProtoUpdate();
        verify(updateTableService, Mockito.times(3)).getProtoMappingFromRemoteURL(protoMappingConfig.getProtoColumnMappingURL(), appConfig.getProtoSchema());
        verify(updateTableService).updateBigQuerySchema(eq(protoMappingConfig.getUpdateBQTableURL()), eq(updateBQTableRequestArgumentCaptor.getValue()));
        ColumnMapping actualNewProtoMapping = protoMappingConfig.getProtoColumnMapping();
        Assert.assertEquals(expectedProtoMapping, actualNewProtoMapping);
    }
}
