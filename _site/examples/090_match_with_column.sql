SELECT row_data->>'col-assignee'  AS dim_0,
       AVG((CASE row_data->>'col-priority'
  WHEN 'critical' THEN 100
  WHEN 'high' THEN 50
  ELSE 0
END)::numeric) AS avg_urgency
FROM rows
WHERE table_id = (SELECT table_id FROM tables WHERE table_name = 'Tasks' AND workspace_id = $1)
GROUP BY dim_0;