SELECT row_data->>'col-priority'  AS dim_0,
       COUNT(*) AS measure
FROM rows
WHERE table_id = 101 AND workspace_id = $1
  AND (row_data->>'col-status' ) != ('merged')
GROUP BY dim_0;
