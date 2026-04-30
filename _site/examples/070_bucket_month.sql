SELECT date_trunc('month', (row_data->>'col-created-at')::timestamptz AT TIME ZONE 'UTC') AS dim_0,
       COUNT(*) AS measure
FROM rows
WHERE table_id = (SELECT table_id FROM tables WHERE table_name = 'Tasks' AND workspace_id = $1)
GROUP BY dim_0
ORDER BY dim_0 ASC;