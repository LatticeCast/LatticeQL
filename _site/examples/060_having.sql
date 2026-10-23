SELECT row_data->>'col-priority'  AS dim_0,
       COUNT(*) AS measure
FROM rows
WHERE table_id = 101
GROUP BY dim_0
HAVING (measure) > (5);
