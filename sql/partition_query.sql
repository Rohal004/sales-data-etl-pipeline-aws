SELECT
    Product,
    Quantity,
    UnitPrice,
    TotalPrice
FROM parquet
WHERE year = 2026
  AND month = 8;
