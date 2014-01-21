package com.linkedin.camus.etl.kafka.common;

import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobContext;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.linkedin.camus.etl.kafka.CamusJob;

public class KafkaBrokerClientTest {

    @Mock
    JobContext context;
    @Mock
    Configuration configuration;
    String name = "testClient";

    @BeforeMethod
    public void setupTest() {
        MockitoAnnotations.initMocks(this);
        when(context.getConfiguration()).thenReturn(configuration);
        when(configuration.get(CamusJob.KAFKA_CLIENT_NAME)).thenReturn(name);
    }

    @Test
    public void testCreatingBroker() throws URISyntaxException {
        String uri = "ip-10-245-61-117.us-west-2.compute.internal:9093";
        KafkaBrokerClient client = KafkaBrokerClient.create(uri, context);
        assertEquals(client.getHost(), "ip-10-245-61-117.us-west-2.compute.internal");
        assertEquals(client.getPort(), 9093);
    }

    @Test
    public void testCreatingBrokerWithNoPortUsesDefaultn() throws URISyntaxException {
        String uri = "ip-10-245-61-117.us-west-2.compute.internal";
        KafkaBrokerClient client = KafkaBrokerClient.create(uri, context);
        assertEquals(client.getHost(), uri);
        assertEquals(client.getPort(), KafkaBrokerClient.DEFAULT_KAFKA_PORT);
    }

}
