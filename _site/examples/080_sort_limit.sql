SELECT row_data->>'col-assignee'  AS dim_0,
       COUNT(*) AS tickets,
       SUM((row_data->>'col-estimate')::numeric) AS workload
FROM rows
WHERE table_id = 'tbl-tasks' AND workspace_id = $1
  AND row_data->>'col-status'  IN ('todo', 'in_progress', 'testing')
GROUP BY dim_0
ORDER BY workload DESC
LIMIT 10;
