SELECT COUNT(*) AS measure
FROM rows
WHERE table_id = 101
  AND row_data @> '{"col-status":"todo"}'::jsonb;
