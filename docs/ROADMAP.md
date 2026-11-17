# Roadmap

1. **Medallion lakehouse** (done). Météo-France daily series for 971/972 through bronze,
   silver and gold Delta tables; the same PySpark code tested locally and run on
   Databricks Free Edition as a serverless job deployed with an Asset Bundle.
2. **Dashboard in Databricks.** An AI/BI dashboard on the gold tables (anomalies, hot days,
   heavy rain, records) shared publicly, linked from the README.
3. **Data quality as code.** Expectations on silver (completeness per station and month,
   ranges, duplicates) stored as a table and shown on the dashboard.
4. **More of the series.** The "autres paramètres" files (humidity, sunshine) and the
   hourly data for the stations of the panel.
5. **Incremental gold.** Rebuild only the months touched by a silver MERGE (Delta change
   data feed) instead of the whole gold layer.
