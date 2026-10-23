SELECT row_data->>'col-assignee'  AS dim_0,
       AVG((CASE row_data->>'col-priority'
  WHEN 'critical' THEN 100
  WHEN 'high' THEN 50
  ELSE 0
END)::numeric) AS avg_urgency
FROM (
  SELECT row_id, row_data
  FROM rows
  WHERE table_id = (
    SELECT table_id FROM tables
    WHERE workspace_id = CAST(:workspace_id AS uuid)
      AND table_name = 'Tasks'
  )
) AS source_rows
GROUP BY dim_0;