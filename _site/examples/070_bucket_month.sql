SELECT date_trunc('month', to_timestamp((row_data->>'col-created-at')::double precision / 1000.0) AT TIME ZONE 'UTC') AS dim_0,
       COUNT(*) AS measure
FROM (
  SELECT row_id, row_data, created_at, updated_at
  FROM rows
  WHERE table_id = (
    SELECT table_id FROM tables
    WHERE workspace_id = CAST(:workspace_id AS uuid)
      AND table_name = 'Tasks'
  )
) AS source_rows
GROUP BY dim_0
ORDER BY dim_0 ASC;