SELECT row_data->>'col-deal-owner'  AS dim_0,
       COUNT(*) FILTER (WHERE row_data @> '{"col-deal-status":"won"}'::jsonb) AS won_count,
       SUM((row_data->>'col-deal-amount')::numeric) FILTER (WHERE row_data @> '{"col-deal-status":"won"}'::jsonb) AS won_value,
       SUM((row_data->>'col-deal-amount')::numeric) FILTER (WHERE row_data @> '{"col-deal-status":"open"}'::jsonb) AS open_value
FROM (
  SELECT row_id, row_data
  FROM rows
  WHERE table_id = (
    SELECT table_id FROM tables
    WHERE workspace_id = CAST(:workspace_id AS uuid)
      AND table_name = 'Deals'
  )
) AS source_rows
GROUP BY dim_0;