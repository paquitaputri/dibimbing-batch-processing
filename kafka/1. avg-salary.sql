CREATE TABLE avg_salary_table as
SELECT department,
    avg(salary) as salary
FROM employee_schema
GROUP BY department EMIT CHANGES;