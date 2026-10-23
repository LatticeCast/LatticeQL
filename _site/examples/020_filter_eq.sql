SELECT COUNT(*) AS measure
FROM (
  SELECT row_id, row_data
  FROM rows
  WHERE table_id = (
    SELECT table_id FROM tables
    WHERE workspace_id = CAST(:workspace_id AS uuid)
      AND table_name = 'Tasks'
  )
) AS source_rows
WHERE row_data @> '{"col-status":"todo"}'::jsonb;