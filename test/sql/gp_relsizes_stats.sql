CREATE EXTENSION gp_relsizes_stats;

DROP TABLE IF EXISTS employees;
CREATE TABLE employees (
    employee_id SERIAL PRIMARY KEY,
    first_name VARCHAR(50) NOT NULL,
    last_name VARCHAR(50) NOT NULL,
    department_id INT,
    date_of_birth DATE
);

INSERT INTO employees (first_name, last_name, department_id, date_of_birth) VALUES
('John', 'Doe', 1, '1988-06-15'),
('Jane', 'Smith', 2, '1990-07-20'),
('Emily', 'Jones', 1, '1985-08-30');

SELECT relsizes_stats_schema.relsizes_collect_stats_once();

SELECT size FROM relsizes_stats_schema.table_sizes_history WHERE relname = 'employees';

-- Fill table with a lot of different rows
DO $$
DECLARE
    i INT;
BEGIN
    FOR i IN 1..10000 LOOP
        INSERT INTO employees (
            first_name, 
            last_name, 
            department_id, 
            date_of_birth
        )
        VALUES (
            'First' || i,                        -- Generate a first name
            'Last' || i,                         -- Generate a last name
            (i % 10) + 1,                        -- Cycle through 10 department IDs
            DATE '1980-01-01' + (i % 365 * 365 / 30) -- Simulated birth dates
        );
    END LOOP;
END $$;

SELECT relsizes_stats_schema.relsizes_collect_stats_once();

-- Check that collected stats are correct
SELECT size FROM relsizes_stats_schema.table_sizes_history WHERE relname = 'employees';

SELECT relsizes_stats_schema.relsizes_collect_stats_once();

-- Validate that after rerun stats collection size of table has not change
SELECT size FROM relsizes_stats_schema.table_sizes_history WHERE relname = 'employees';
