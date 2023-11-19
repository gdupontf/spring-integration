package org.springframework.integration.jdbc.sqlserver;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import javax.sql.DataSource;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

// TODO: configure test container with database name
@SpringJUnitConfig
@DirtiesContext
public class SqlServerTests implements SqlServerContainerTest {

	@Autowired
	DataSource dataSource;

	static String SEND_STATEMENT = """
			USE testdb;
			DECLARE @dialog_handle UNIQUEIDENTIFIER;
			DECLARE @message NVARCHAR(max) = ?;
			BEGIN DIALOG @dialog_handle
			    FROM SERVICE MessagesProducer
			    TO SERVICE 'MessagesConsumer'
			    ON CONTRACT [//org/springframework/integration/Message/SimpleContract]
			    WITH ENCRYPTION = OFF;
			SEND ON CONVERSATION @dialog_handle MESSAGE TYPE [//org/springframework/integration/Message] (@Message) ;
			""";
	public static final String RECEIVE_STATEMENT = """
			USE testdb;
			RECEIVE *, cast(message_body as NVARCHAR) as converted_body
			FROM testschema.TestConsumerQueue;
			""";

	@Test
	public void simpleMessage_transactional_isConsumed() throws Exception {

		// enqueue message
		Connection connection = dataSource.getConnection();
		try (PreparedStatement enqueueStatement = connection.prepareStatement(SEND_STATEMENT)) {
			enqueueStatement.setString(1, "TEST_MESSAGE");
			boolean sent = enqueueStatement.execute();
			// TODO: find something concrete to assert on
			assertThat(sent).as("No resultset is obviously returned")
							.isFalse();
			connection.commit();
		}
		// dequeue message
		try (PreparedStatement dequeueStatement = connection.prepareStatement(RECEIVE_STATEMENT)) {
			dequeueStatement.execute();
			ResultSet resultSet = dequeueStatement.getResultSet();
			boolean first = resultSet.next();
			assertThat(first).as("At least one message should have been produced")
							 .isTrue();
			assertThat(resultSet.getString("converted_body")).isEqualTo("TEST_MESSAGE");
			assertThat(resultSet.next()).as("Only one result should be expected from a receive")
										.isFalse();
			connection.commit();
		}

		// message has been removed
		try (PreparedStatement dequeueStatement = connection.prepareStatement(RECEIVE_STATEMENT)) {
			dequeueStatement.execute();
			ResultSet resultSet = dequeueStatement.getResultSet();
			boolean first = resultSet.next();
			assertThat(first).as("No more message should be enqueued")
							 .isFalse();
		}
	}

	@Test
	public void simpleMessage_transactional_enqueueFails_noMessage() throws Exception {
		// enqueue message
		Connection connection = dataSource.getConnection();
		try (PreparedStatement enqueueStatement = connection.prepareStatement(SEND_STATEMENT)) {
			enqueueStatement.setString(1, "TEST_MESSAGE");
			boolean sent = enqueueStatement.execute();
			// TODO: find something concrete to assert on
			assertThat(sent).as("No resultset is obviously returned")
							.isFalse();
			// "fail" enqueue
			connection.rollback();
		}
		// expect no message
		try (PreparedStatement dequeueStatement = connection.prepareStatement(RECEIVE_STATEMENT)) {
			dequeueStatement.execute();
			ResultSet resultSet = dequeueStatement.getResultSet();
			boolean first = resultSet.next();
			assertThat(first).as("No message should have been produced")
							 .isFalse();
		}
	}

	@Test
	public void simpleMessage_transactional_dequeueFails_noMessage() throws Exception {
		// enqueue message
		Connection connection = dataSource.getConnection();
		try (PreparedStatement enqueueStatement = connection.prepareStatement(SEND_STATEMENT)) {
			enqueueStatement.setString(1, "TEST_MESSAGE");
			boolean sent = enqueueStatement.execute();
			// TODO: find something concrete to assert on
			assertThat(sent).as("No resultset is obviously returned")
							.isFalse();
			// commit enqueue
			connection.commit();
		}

		// dequeue message
		try (PreparedStatement dequeueStatement = connection.prepareStatement(RECEIVE_STATEMENT)) {
			dequeueStatement.execute();
			ResultSet resultSet = dequeueStatement.getResultSet();
			boolean first = resultSet.next();
			assertThat(first).as("At least one message should have been produced")
							 .isTrue();
			assertThat(resultSet.getString("converted_body")).isEqualTo("TEST_MESSAGE");
			assertThat(resultSet.next()).as("Only one result should be expected from a receive")
										.isFalse();
			// "fail" consuming
			connection.rollback();
		}
		// message was requeued
		try (PreparedStatement dequeueStatement = connection.prepareStatement(RECEIVE_STATEMENT)) {
			dequeueStatement.execute();
			ResultSet resultSet = dequeueStatement.getResultSet();
			boolean first = resultSet.next();
			assertThat(first).as("At least one message should have been produced")
							 .isTrue();
			assertThat(resultSet.getString("converted_body")).isEqualTo("TEST_MESSAGE");
			assertThat(resultSet.next()).as("Only one result should be expected from a receive")
										.isFalse();
			connection.commit();
		}
		// dequeue again and expect dequeue to have consumed
		try (PreparedStatement dequeueStatement = connection.prepareStatement(RECEIVE_STATEMENT)) {
			dequeueStatement.execute();
			ResultSet resultSet = dequeueStatement.getResultSet();
			boolean first = resultSet.next();
			assertThat(first).as("No more message should be enqueued")
							 .isFalse();
		}
	}

	@Configuration
	public static class Config {

		@Bean
		public DataSource datasource() {
			return SqlServerContainerTest.dataSource();
		}
	}
}
