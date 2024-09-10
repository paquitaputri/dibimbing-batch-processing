CREATE STREAM employee_schema (
    emp_id varchar,
    employee_name varchar,
    department varchar,
    state varchar,
    salary int,
    age int,
    bonus int,
    ts bigint,
    new boolean
) WITH (
    KAFKA_TOPIC = 'test-topic-1',
    VALUE_FORMAT = 'JSON'
);