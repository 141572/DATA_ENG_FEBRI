# 🚀 PANDUAN CEPAT: MENJALANKAN PIPELINE DI LAPTOP BARU

Panduan ini berisi langkah-langkah praktis dan urutan perintah terminal untuk menjalankan seluruh **E-commerce Data Engineering Pipeline** dari awal hingga selesai pada laptop baru.

---

## 📋 1. Prasyarat Sistem (Prerequisites)
Sebelum memulai, pastikan laptop baru sudah menginstal perangkat lunak berikut:
1.  **Docker Desktop** (dengan integrasi WSL 2 aktif).
2.  **Git** (untuk clone repositori).

---

## ⚙️ 2. Persiapan Awal (Setup)

### Langkah A: Salin Repositori & Masuk ke Folder Proyek
Buka terminal (PowerShell / Command Prompt / Bash) di laptop Anda, lalu jalankan:
```bash
git clone <url-repositori-anda>
cd data_eng
```

### Langkah B: Duplikat File Environment Konfigurasi
Jalankan perintah ini untuk membuat konfigurasi `.env` default proyek:
```bash
# Untuk Windows PowerShell:
Copy-Item .env.example .env

# Untuk Windows CMD / Linux / macOS:
cp .env.example .env
```

---

## 🐳 3. Inisialisasi Klaster Docker

Jalankan perintah berikut untuk menyalakan seluruh kontainer layanan (Kafka, Spark, Airflow, PostgreSQL) secara background:
```bash
docker compose up -d
```
*   *Catatan:* Proses download image Docker pertama kali akan memakan waktu 3-5 menit tergantung kecepatan internet Anda.
*   **Pengecekan Status Kontainer:** Pastikan semua kontainer sehat sebelum melangkah ke tahap berikutnya:
    ```bash
    docker compose ps
    ```

---

## ⚡ 4. Urutan Eksekusi Pipeline (ETL End-to-End)

Jalankan ketiga perintah di bawah ini secara berurutan pada terminal host laptop Anda untuk memproses data dari hulu ke hilir:

### TAHAP 1: Ingesti Data Aliran (Kafka Producer Ingestion)
Mengalirkan dataset CSV mentah dari folder lokal `./dataset/` ke broker Kafka secara streaming:
```bash
docker exec -it airflow-scheduler python/scripts/kafka_producer.py
```
*(Tunggu hingga muncul log pengiriman data sukses selesai).*

### TAHAP 2: Pemrosesan & Transformasi Data (PySpark Engine)
Mengkonsumsi data dari topik Kafka, membersihkan duplikasi, membagi data ke skema bintang (Star Schema), dan menyimpannya ke format Parquet:
```bash
docker exec -it spark-master spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
  /workspace/scripts/spark_transform.py
```
*(Tunggu sekitar 1 menit hingga muncul pesan: "All transformations and Parquet writes completed successfully!").*

### TAHAP 3: Pemuatan Aman ke PostgreSQL DWH (PySpark Loader)
Mengosongkan database secara aman (*cascade*) dan memuat tabel Parquet secara sekuensial ke dalam database PostgreSQL:
```bash
docker exec -it spark-master spark-submit \
  --packages org.postgresql:postgresql:42.6.0 \
  /workspace/scripts/spark_load.py
```
*(Tunggu hingga muncul tulisan: "ALL TRANSACTIONS AND DIMENSIONS SUCCESSFULLY LOADED INTO POSTGRES DWH!").*

---

## 📊 5. Verifikasi & Menampilkan Hasil Analisis (KPIs)

Untuk membuktikan data sudah terisi penuh dan views analitis berjalan sempurna:

1.  **Masuk ke terminal PostgreSQL di dalam kontainer:**
    ```bash
    docker exec -it postgres psql -U postgres -d olist_dwh
    ```
2.  **Tampilkan Laporan KPI Finansial Utama (Revenue & AOV Bulanan):**
    ```sql
    SELECT date, total_orders, net_product_revenue, average_order_value_aov 
    FROM view_kpi_financial_metrics 
    ORDER BY net_product_revenue DESC 
    LIMIT 5;
    ```
3.  **Tampilkan Laporan Distribusi Penjualan Wilayah (Geospatial):**
    ```sql
    SELECT state, city, order_count, total_sales 
    FROM view_geospatial_sales_distribution 
    ORDER BY total_sales DESC 
    LIMIT 5;
    ```
4.  **Keluar dari PostgreSQL CLI:**
    ```sql
    \q
    ```

---

## 🔌 6. Mematikan Klaster
Jika presentasi atau pengujian selesai dan ingin menghemat RAM laptop Anda, matikan seluruh kontainer secara aman tanpa menghapus datanya dengan perintah:
```bash
docker compose down
```
*(Saat dinyalakan kembali menggunakan `docker compose up -d`, seluruh data Anda tetap tersimpan utuh di komputer).*
