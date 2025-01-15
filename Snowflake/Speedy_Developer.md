#### Multiple ways to speed up Snowflake Development

Limit function

```
SET DEBUG = 1;
WITH data AS ( SELECT * FROM your_table WHERE some_condition = 'value' )
SELECT * FROM data LIMIT CASE WHEN ${DEBUG} = 1 THEN 500 ELSE NULL END;
```

Conditional SQL Execution

```
SET DEBUG = 1;

BEGIN
    IF ${DEBUG} = 1 THEN
        EXECUTE IMMEDIATE $$ 
            SELECT *
            FROM your_table
            WHERE some_condition = 'value'
            LIMIT 500
        $$;
    ELSE
        EXECUTE IMMEDIATE $$
            SELECT *
            FROM your_table
            WHERE some_condition = 'value'
        $$;
    END IF;
END;
```

Inner Join

```
SET DEBUG = 1;

SELECT t1.*
FROM table1 t1
LEFT JOIN table2 t2
    ON t1.common_column = t2.common_column
    AND ${DEBUG} = 1; -- The join condition is only applied if DEBUG=1
```
