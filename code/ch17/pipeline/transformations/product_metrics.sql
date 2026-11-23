-- A metric view in a pipeline SQL file.
CREATE VIEW product_metrics
WITH METRICS
LANGUAGE YAML
AS $$
version: 0.1
source: products
dimensions:
  - name: category
    expr: category
measures:
  - name: products
    expr: COUNT(1)
$$;
