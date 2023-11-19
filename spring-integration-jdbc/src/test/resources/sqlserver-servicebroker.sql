create database testdb;

use testdb;

create schema testschema;

ALTER DATABASE testdb
    SET ENABLE_BROKER;

CREATE QUEUE testschema.TestProducerQueue;
CREATE QUEUE testschema.TestConsumerQueue;


CREATE MESSAGE TYPE
    [//org/springframework/integration/Message]
    VALIDATION = NONE;

CREATE CONTRACT
    [//org/springframework/integration/Message/SimpleContract]
    ([//org/springframework/integration/Message]
    SENT BY INITIATOR
    );
CREATE SERVICE MessagesProducer
    ON QUEUE testschema.TestProducerQueue ([//org/springframework/integration/Message/SimpleContract]);

CREATE SERVICE
    MessagesConsumer
    ON QUEUE testschema.TestConsumerQueue ([//org/springframework/integration/Message/SimpleContract]);