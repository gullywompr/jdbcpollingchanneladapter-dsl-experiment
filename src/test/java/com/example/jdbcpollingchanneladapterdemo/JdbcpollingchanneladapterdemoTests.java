package com.example.jdbcpollingchanneladapterdemo;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;

import java.sql.ResultSet;
import java.sql.SQLException;
import javax.sql.DataSource;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.autoconfigure.jdbc.AutoConfigureTestDatabase;
import org.springframework.boot.test.autoconfigure.jdbc.JdbcTest;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.integration.config.EnableIntegration;
import org.springframework.integration.core.MessageSource;
import org.springframework.integration.dsl.IntegrationFlowAdapter;
import org.springframework.integration.dsl.IntegrationFlowDefinition;
import org.springframework.integration.dsl.context.IntegrationFlowContext;
import org.springframework.integration.dsl.core.Pollers;
import org.springframework.integration.jdbc.JdbcPollingChannelAdapter;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.messaging.PollableChannel;
import org.springframework.messaging.support.GenericMessage;
import org.springframework.test.context.jdbc.Sql;
import org.springframework.test.context.jdbc.Sql.ExecutionPhase;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@SpringBootTest
@AutoConfigureTestDatabase
@JdbcTest
@EnableIntegration
@ComponentScan
@EnableAutoConfiguration
@EnableConfigurationProperties
public class JdbcpollingchanneladapterdemoTests {

  @Autowired
  private IntegrationFlowContext genericFlowContext;

  @Autowired
  private IntegrationFlowContext jdbcPollingFlowContext;

  @Autowired
  private BeanFactory beanFactory;

  @Test
  @Sql(executionPhase = ExecutionPhase.BEFORE_TEST_METHOD,
      statements = "Create Table DEMO (CODE VARCHAR(5));")
  @Sql(executionPhase = ExecutionPhase.BEFORE_TEST_METHOD,
      statements = "Insert into DEMO (CODE) VALUES ('12345');")
  public void Should_HaveMessageOnTheQueue_When_UnsentDemosIsInTheDatabase() {

    this.genericFlowContext.registration(new GenericFlowAdapter()).register();

    PollableChannel genericChannel = this.beanFactory.getBean("GenericFlowAdapterOutput",
        PollableChannel.class);

    this.jdbcPollingFlowContext.registration(new JdbcPollingFlowAdapter()).register();

    PollableChannel jdbcPollingChannel = this.beanFactory.getBean("JdbcPollingFlowAdapterOutput",
        PollableChannel.class);

    // works OK
    assertThat(genericChannel.receive(5000).getPayload(), equalTo("15317"));

    // currently giving null pointer exception
    assertThat(jdbcPollingChannel.receive(5000).getPayload(), equalTo("15317"));
  }

  private static class GenericFlowAdapter extends IntegrationFlowAdapter {

    @Override
    protected IntegrationFlowDefinition<?> buildFlow() {
      return from(getObjectMessageSource(),
          e -> e.poller(Pollers.fixedRate(100)))
          .channel(c -> c.queue("GenericFlowAdapterOutput"));
    }

    private MessageSource<Object> getObjectMessageSource() {
      return () -> new GenericMessage<>("15317");
    }
  }

  private static class JdbcPollingFlowAdapter extends IntegrationFlowAdapter {

    @Autowired
    @Qualifier("dataSource")
    DataSource dataSource;

    @Override
    protected IntegrationFlowDefinition<?> buildFlow() {
      return from(getObjectMessageSource(),
          e -> e.poller(Pollers.fixedRate(100)))
          .channel(c -> c.queue("JdbcPollingFlowAdapterOutput"));
    }

    private MessageSource<Object> getObjectMessageSource() {
      JdbcPollingChannelAdapter adapter =
          new JdbcPollingChannelAdapter(dataSource, "SELECT CODE FROM DEMO");
      adapter.setRowMapper(new DemoRowMapper());
      adapter.setMaxRowsPerPoll(1);
      return adapter;
    }
  }

  private static class Demo {

    private String demo;

    String getDemo() {return demo;}

    void setDemo(String value) {
      this.demo = value;
    }

    @Override
    public String toString() {
      return this.demo;
    }
  }

  public static class DemoRowMapper implements RowMapper<Demo> {

    @Override
    public Demo mapRow(ResultSet rs, int rowNum) throws SQLException {
      Demo demo = new Demo();
      demo.setDemo(rs.getString("CODE"));
      return demo;
    }
  }
}
