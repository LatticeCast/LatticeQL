SELECT row_data->>'col-deal-owner'  AS dim_0,
       COUNT(*) FILTER (WHERE row_data @> '{"col-deal-status":"won"}'::jsonb) AS won_count,
       SUM((row_data->>'col-deal-amount')::numeric) FILTER (WHERE row_data @> '{"col-deal-status":"won"}'::jsonb) AS won_value,
       SUM((row_data->>'col-deal-amount')::numeric) FILTER (WHERE row_data @> '{"col-deal-status":"open"}'::jsonb) AS open_value
FROM rows
WHERE table_id = 'tbl-deals' AND workspace_id = $1
GROUP BY dim_0;
