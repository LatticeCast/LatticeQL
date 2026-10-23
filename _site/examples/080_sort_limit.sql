SELECT row_data->>'col-assignee'  AS dim_0,
       COUNT(*) AS tickets,
       SUM((row_data->>'col-estimate')::numeric) AS workload
FROM (
  SELECT row_id, row_data
  FROM rows
  WHERE table_id = (
    SELECT table_id FROM tables
    WHERE workspace_id = CAST(:workspace_id AS uuid)
      AND table_name = 'Tasks'
  )
) AS source_rows
WHERE row_data->>'col-status'  IN ('todo', 'in_progress', 'testing')
GROUP BY dim_0
ORDER BY workload DESC
LIMIT 10;