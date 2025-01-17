#### Shrink Table Size to Speed Up Development
The goal of this page is to establish best practices to speed up development time.  These techniques are rated into three categories:

1) Quick and Dirty - Good for small searches. Takes only a few seconds to limit.
2) Pareto Rule - 20% of the benefit and 80% of the performance.
3) Full Ham - >99% of the theoretical benefit.



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
