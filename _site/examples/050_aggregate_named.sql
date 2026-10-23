SELECT row_data->>'col-priority'  AS dim_0,
       COUNT(*) AS count,
       AVG((row_data->>'col-estimate')::numeric) AS avg_pts,
       SUM((row_data->>'col-estimate')::numeric) AS total_pts
FROM rows
WHERE table_id = 101 AND workspace_id = $1
  AND row_data @> '{"col-type":"task"}'::jsonb
GROUP BY dim_0;
