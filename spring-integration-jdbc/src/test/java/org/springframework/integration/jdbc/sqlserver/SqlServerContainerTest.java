package org.springframework.integration.jdbc.sqlserver;

import org.apache.commons.dbcp2.BasicDataSource;
import org.junit.jupiter.api.BeforeAll;
import org.testcontainers.containers.MSSQLServerContainer;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import javax.sql.DataSource;

@Testcontainers(disabledWithoutDocker = true)
public interface SqlServerContainerTest {

	String SQL_SERVER_IMAGE_NAME = "mcr.microsoft.com/mssql/server:2022-preview-ubuntu-22.04";
	// tolerate sql server's circular generics and testcontainers' resource management
	@SuppressWarnings({"rawtypes", "resource"})
	MSSQLServerContainer MSSQL_SERVER_CONTAINER = (MSSQLServerContainer) new MSSQLServerContainer(
			DockerImageName.parse(SQL_SERVER_IMAGE_NAME))
//			.withUrlParam("databaseName", "testdb")
														 .withInitScript(
																 "sqlserver-servicebroker.sql")
														 .withEnv("ACCEPT_EULA", "Y");

	@BeforeAll
	static void startContainer() {
		MSSQL_SERVER_CONTAINER.start();
	}

	static DataSource dataSource() {
		BasicDataSource dataSource = new BasicDataSource();
		dataSource.setDriverClassName(MSSQL_SERVER_CONTAINER.getDriverClassName());
		String jdbcUrl = MSSQL_SERVER_CONTAINER.getJdbcUrl();
		String url = "%s;databaseName=%s;".formatted(jdbcUrl, "testdb");
		dataSource.setUrl(url);
		dataSource.setUsername(MSSQL_SERVER_CONTAINER.getUsername());
		dataSource.setPassword(MSSQL_SERVER_CONTAINER.getPassword());
		dataSource.setDefaultSchema("testschema");
		dataSource.setDefaultAutoCommit(false);
		return dataSource;
	}
}
