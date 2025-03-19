SELECT 
    id, 
    COALESCE(first, '') AS first, 
    COALESCE(second, '') AS second
FROM (
    SELECT id, ARRAY_AGG(first IGNORE NULLS)[SAFE_OFFSET(0)] AS first, 
                ARRAY_AGG(second IGNORE NULLS)[SAFE_OFFSET(0)] AS second
    FROM your_table
    GROUP BY id
);

SELECT 
    id, 
    ARRAY_AGG(NULLIF(first, '') IGNORE NULLS)[SAFE_OFFSET(0)] AS first,
    ARRAY_AGG(NULLIF(second, '') IGNORE NULLS)[SAFE_OFFSET(0)] AS second
FROM your_table
GROUP BY id;
