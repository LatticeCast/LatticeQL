SELECT AVG(CASE row_data->>'col-status'
  WHEN 'done' THEN 1
  WHEN 'merged' THEN 1
  ELSE 0
END) FILTER (WHERE row_data->>'col-status'  IN ('done', 'merged')) AS rate
FROM rows
WHERE table_id = (SELECT table_id FROM tables WHERE table_name = 'Tasks' AND workspace_id = $1)
  AND (row_data->>'col-sprint' ) = ($2);