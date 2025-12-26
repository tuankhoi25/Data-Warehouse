# Data Warehouse with DBT

## Mục lục
1. [Đúc kết](#1-đúc-kết)
    - 1.1. [Data WareHouse Architecture](#11-data-warehouse-architecture)
    - 1.2. [ETL/ELT trade off](#12-etlelt-trade-off)
    - 1.3. [DBT (Data Build Tool)](#13-dbt-data-build-tool)
    - 1.4. [ClickHouse](#14-clickhouse)
    - 1.5. [OpenMetadata](#15-openmetadata)
2. [Áp dụng với bộ dữ liệu mẫu](#2-áp-dụng-với-bộ-dữ-liệu-mẫu)
    - 2.1. [Bộ dữ liệu](#21-bộ-dữ-liệu-tái-sử-dụng-từ-repo-lakehouse-cũ)
    - 2.2. [Project Structure](#22-project-structure)

---

## 1. Đúc kết

### 1.1. Data WareHouse Architecture

<p align="center">
  <img src="https://github.com/user-attachments/assets/56cc176a-c043-481d-b6d4-cc0c1e330ebd" alt="Data Warehouse Layers">
</p>
<p align="center">
  <strong>Data Warehouse Layers</strong>
</p>


1. Lading/Ingestion/Raw Layer
    - Dùng để lưu trữ dữ liệu lịch sử để dựa vào đó để kiếm toán, khôi phục dữ liệu, tách biệt với data source.
    - Kiểu dữ liệu phù hợp để lưu như CSV / JSON / Parquet (nếu là table thì export để lưu, không quan tâm schema)
    - Nếu quy mô dữ liệu lớn thì sẽ lưu trên Object Storage (S3), nếu nhỏ thì có thể lưu vào 1 schema ở trong Data WareHouse tool (<database_name>.<schema_name>.<table_name>)
    - Lưu data như đã lấy từ data source (không thay đổi gì)
    - Thêm metadata column để phục vụ quản lý, kiểm soát, truy vết
    - Incremental/Full load
2. Bronze/Staging Layer
    - Tầng trung gian để thực hiện những transformation cơ bản để chuẩn hoá dữ liệu sắp dùng (select, rename, data type casting, basic transform)
    - Không thực hiện transformation có business logic
    - Có thể là View/Table (có trade off)
    - Đây là tầng chuẩn hoá (chung cho cả tổ chức) nên sẽ tập trung chia theo data source thay vì chia theo department
3. Cleansed/Silver/Intermediate
    - Thực hiện các transformation phức tạp (join, aggregate) dựa trên business logic
    - Có thể chia theo department nếu có
4. Gold/Serving
    - Dim/Fact modelling (tối ưu phân tích theo groupby, agg)
    - Star/Snowflake Schema
    - Slowly Changing Dimension
5. Semantic/Presentation/Marts
    - Tính toán sẵn những kpi/metrics mà consumer (BI tool) cần dùng
    - Có thể lưu thành Materialized View (tạo 1 lần đọc nhiều lần) vì chi phí lưu trữ ít hơn chi phí tính toán

Tổng quan:
  - Có thể thêm/bớt layer tuỳ theo nhu cầu sử dụng
  - Dù có nhiều tên gọi khác nhau nhưng mục đích thì giống nhau
  - Chia ra nhiều layer để tách biệt ra để dễ xử lý, debug, maintain

### 1.2. ETL/ELT trade off

ELT:

- Đặc điểm: Data được extract, sau đó load vào Data WareHouse và sau đó được transform bằng Data WareHouse luôn
- Ưu điểm: Vì transform và được consume (BI consume bằng query) được thực hiện trên cùng Data Warehouse. Ta sẽ nâng resource cho Data Warehouse để nó handle, khi không transform hoặc bị query nhiều thì resource sẽ được toàn quyền dùng bởi hoạt động còn lại -> nhanh hơn, tối ưu resource hơn
- Nhược điểm: vì dùng chung resource nên lúc cao điểm có thể bị chồng chéo làm ảnh hưởng trải nghiệm người dùng

ETL:

- Đặc điểm: Data được extract, sau đó được transform bởi 1 tool khác rồi mới được load vào Data WareHouse
- Ưu điểm: dù pipeline chạy nhiều hay Data WareHouse được dùng nhiều thì 2 cái không ăn chéo resource của nhau
- Nhược điểm: nếu không transform data hoặc Data Warehouse không được query thì resource sẽ rảnh -> lãng phí

--> Tuỳ trường hợp sử dụng (ELT ra đời do khả năng tính toán của Data WareHouse đã trở nên mạnh hơn như Clickhouse, Apache Doris - Trade off là resource)

### 1.3. DBT (Data Build Tool)

Lợi ích:

- Tăng chất lượng dữ liệu nhờ có hỗ trợ Data Quality Test (cơ bẩn, có thể custom, có thể dùng 3th party như Great-Expectation)
- Hỗ trợ Data Lineage / Document (giúp truy vết dữ liệu, hiểu rõ dữ liệu)
- DBT tự động hiểu quan hệ phụ thuộc giữa các table/model nhờ ref() function
- Ép buộc tổ chức code tốt hơn (mỗi script xử lý cho một table chỉ được định nghĩa trong một model script, tránh việc 1 script .sql chứa logic xử lý cho hàng trăm bảng)
- Tổ chức code hiệu quả hơn: các model cùng nằm trong /folder/<sub_folder> có thể có cùng kiểu cấu hình (materialized, ...)
- Dev dễ hơn:
  - profile.yml có hỗ trợ chia connection theo dev/prod, dùng --target là đc
  - Dùng materialized=incremental giúp incremental processing dễ hơn (có cả incremental_strategy)
- ... nhiều lợi ích khác

### 1.4. ClickHouse

Hiện chỉ đang triển khai vội để viết pipeline (chưa tìm hiểu kỹ để tối ưu)

### 1.5. OpenMetadata

Dự định dùng OpenMetadata làm Data Catalog để xem Data Lineag

dbt-colibri là dự án open source giúp hiển thị dbt document/lineage lightwieght (chưa áp dụng thì chưa hỗ trợ cho dbt-clickhouse)

## 2. Áp dụng với bộ dữ liệu mẫu

Xây dựng Data Warehouse với DBT cho batch processing để xử lý dữ liệu bằng ELT pipeline

### 2.1. Bộ dữ liệu (Tái sử dụng từ repo LakeHouse cũ)

Bộ dữ liệu sẽ là sự kết hợp của nhiều nguồn khác nhau, bao gồm:

- Bộ dữ liệu về các đánh giá sản phẩm được cung cấp bởi Amazon tại [Kaggle](https://www.kaggle.com/datasets/cynthiarempel/amazon-us-customer-reviews-dataset).
- Kết hợp với các thông tin của người dùng được tạo ra bằng thư viện [Faker](https://faker.readthedocs.io/en/master/index.html#).
- Bộ dữ liệu về thành phố, tiểu bảng, mã zip code của Mỹ được cung cấp bởi [SimpleMaps](https://simplemaps.com/data/us-zips)

**Lưu ý**: vì vấn đề bản quyền của bộ dữ liệu của Amazon trên Kaggle nên không public hầu hết các bảng dữ liệu.

<p align="center">
  <img src="https://github.com/user-attachments/assets/06042d19-1d8a-4b3c-8a28-035ed5ea7dac" alt="OLTP Data Model">
</p>
<p align="center">
  <strong>Đây là bộ dữ liệu sau khi được chuẩn hoá (OLTP)</strong>
</p>

<br><br>

<p align="center">
  <img src="https://github.com/user-attachments/assets/95fe790a-125c-4e70-99a3-87a8dbbb9e5f" alt="Raw Layer">
</p>
<p align="center">
  <strong>Đây là cấu trúc ở Raw Layer</strong>
</p>

<br><br>

<p align="center">
  <img src="https://github.com/user-attachments/assets/a64eb22f-3634-42de-a971-73cd16b3a0ed" alt="Staging Layer">
</p>
<p align="center">
  <strong>Đây là cấu trúc ở Staging Layer</strong>
</p>

<br><br>

<p align="center">
  <img src="https://github.com/user-attachments/assets/15baab37-040f-48c7-8813-0968582e77af" alt="Marts Layer">
</p>
<p align="center">
  <strong>Đây là cấu trúc ở Marts Layer</strong>
</p>

<br><br>

<p align="center">
  <img src="https://github.com/user-attachments/assets/69e74cba-72d5-41b9-a57f-d2f3922c3de0" alt="Lakehouse Architecture">
</p>
<p align="center">
  <strong>Data Platform Architecture</strong>
</p>


## 2.2. Project Structure

Tổ chức DBT theo quy ước [DBT Convention](https://docs.getdbt.com/best-practices/how-we-structure/1-guide-overview)

``` bash
.
├── README.md
├── airflow                       # Đưa DBT folder vô để 
│   ├── dags                      
│   │   ├── amazon_review.py      # DAG trigger DBT CLI
│   │   └── dbt_project           # DBT project, tổ chức theo quy ước
│   │       ├── dbt_project.yml
│   │       ├── macros
│   │       ├── models
│   │       │   ├── intermediate
│   │       │   ├── marts
│   │       │   │   └── core
│   │       │   ├── raw
│   │       │   │   └── postgres
│   │       │   └── staging
│   │       │       └── postgres
│   │       ├── packages.yml
│   │       ├── profiles.yml
│   │       └── tests
├── data_platform_config          # Nơi config các service trong docker-compose.yml 
├── ddl_script                    # Initial script cho CLickhouse
├── docker                        # Chứa customer image
├── docker-compose.yml
└── requirements.txt
```
