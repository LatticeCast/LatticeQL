SELECT AVG(CASE row_data->>'col-status'
  WHEN 'done' THEN 1
  WHEN 'merged' THEN 1
  ELSE 0
END) FILTER (WHERE row_data->>'col-status'  IN ('done', 'merged')) AS rate
FROM (
  SELECT row_id, row_data, created_at, updated_at
  FROM rows
  WHERE table_id = (
    SELECT table_id FROM tables
    WHERE workspace_id = CAST(:workspace_id AS uuid)
      AND table_name = 'Tasks'
  )
) AS source_rows
WHERE (row_data->>'col-sprint' ) = ($1);