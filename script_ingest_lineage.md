Vì Openmetadata mặc định nhận dạng thông tin theo 3 cấp `<database>.<schema>.<table>` nhưng Clickhouse chỉ có `<database>.<table>`. Do đó, sau khi chạy `dbt run`, `dbt docs generate` thì DBT sẽ tự tạo ra file `manifest.json` và `catalog.json` (thiếu `<schema>`), ta sẽ chỉnh sửa thủ công 2 file đó, thay đổi thông tin nhận dạng cho phù hợp. Cuối cùng là đẩy metadata lên OpenMetadata

```bash
jq '.nodes[] |= (if .resource_type == "model" then 
  (if (.database == "" or .database == null) then .schema else .database end) as $val | 
  .database = "default" | 
  .schema = $val |
  .relation_name = ("`default`.`" + $val + "`.`" + .name + "`")
else . end)' airflow/dags/dbt_project/target/manifest.json > ./manifest.json
```

```bash
jq '.nodes[] |= (if .metadata then 
  (if (.metadata.database == "" or .metadata.database == null) then .metadata.schema else .metadata.database end) as $val | 
  .metadata.database = "default" | 
  .metadata.schema = $val 
else . end)' airflow/dags/dbt_project/target/catalog.json > ./catalog.json
```

```bash
chmod 777 ./manifest.json ./catalog.json
```

```bash
metadata ingest -c ./dbt_local_config.yaml
```