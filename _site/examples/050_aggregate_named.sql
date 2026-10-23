SELECT row_data->>'col-priority'  AS dim_0,
       COUNT(*) AS count,
       AVG((row_data->>'col-estimate')::numeric) AS avg_pts,
       SUM((row_data->>'col-estimate')::numeric) AS total_pts
FROM (
  SELECT row_id, row_data, created_at, updated_at
  FROM rows
  WHERE table_id = (
    SELECT table_id FROM tables
    WHERE workspace_id = CAST(:workspace_id AS uuid)
      AND table_name = 'Tasks'
  )
) AS source_rows
WHERE row_data @> '{"col-type":"task"}'::jsonb
GROUP BY dim_0;