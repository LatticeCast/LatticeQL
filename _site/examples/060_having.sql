SELECT row_data->>'col-priority'  AS dim_0,
       COUNT(*) AS measure
FROM rows
WHERE table_id = 'tbl-tasks' AND workspace_id = $1
GROUP BY dim_0
HAVING (COUNT(*)) > (5);
