SELECT row_data->>'col-priority'  AS dim_0,
       COUNT(*) AS measure
FROM rows
WHERE table_id = (SELECT table_id FROM tables WHERE table_name = 'Tasks' AND workspace_id = $1)
  AND (row_data->>'col-status' ) != ('merged')
GROUP BY dim_0;