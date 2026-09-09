SELECT COUNT(*) AS measure
FROM rows
WHERE table_id = 'tbl-tasks' AND workspace_id = $1
  AND row_data @> '{"col-status":"todo"}'::jsonb;
