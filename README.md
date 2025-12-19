# 🧩 UTS Sistem Terdistribusi – Pub/Sub Aggregator

Proyek ini merupakan implementasi **Event Aggregator Service** berbasis **FastAPI + asyncio**, yang berfungsi sebagai sistem **Pub/Sub sederhana** dengan fitur **deduplication, idempotency, dan event persistence**.  

---

## Fitur Utama

- **Publish / Subscribe Model**
  - Endpoint `POST /publish` menerima satu atau beberapa event.
- **Deduplication & Idempotency**
  - Setiap event diidentifikasi unik berdasarkan kombinasi `(topic, event_id)`.
  - Event duplikat tidak akan diproses ulang, bahkan setelah restart (persisten di SQLite).
- **Monitoring & Statistik**
  - `GET /events?topic=...` → menampilkan daftar event unik yang telah diproses.
  - `GET /stats` → menampilkan metrik sistem seperti:
    - Total event diterima  
    - Event unik diproses  
    - Duplikasi terdeteksi & di-drop  
    - Daftar topik  
    - Uptime server  
  
  
---

## Tech Stack

- Python 3.11
- SQLite
- FastAPI, dan
- ***library lainnnya di requirements.txt***

---

## Cara mengaktifkan (Docker Compose)

- ***Clone repository***
  ```
  git clone https://github.com/azureeeeeeeeeeee/uts-pub-sub-aggregator.git
  ```
- ***Change directory*** ke folder baru
  ```
  cd path/to/uts-pub-sub-aggregator
  ```
- *****Build docker compose*****
  ```
  Build docker compose --build
  ```
- Akses app pada ***url***
  ```
  http://localhost:5000/
  ```

---

## Cara mengaktifkan (Localhost tanpa docker)

- ***Clone repository***
  ```
  git clone https://github.com/azureeeeeeeeeeee/uts-pub-sub-aggregator.git
  ```
- ***Change directory*** ke folder baru
  ```
  cd path/to/uts-pub-sub-aggregator
  ```
- Buat ***virtual environment***
  ```
  python -m venv venv
  ```
- Aktifkan venv  
  powershell
  ```
  ./path/to/venv/scripts/activate
  ```
  bash
  ```
  source path/to/venv/scripts/activate
  ```

- Install ***dependensi***  
  ```
  pip install -r requirements.txt
  ```

- Jalankan aplikasi
  ```
  uvicorn src.main:app
  ```

- Akses aplikasi di
  ```
  http://localhost:8000/
  ```

- Jalankan unittest (optional)  
  ```
  pytest -v tests/test_app.py
  ```

- Jalankan stress test (optional)  
  ```
  python tests/stress_test.py
  ```

---

## Request & Response

- ### /publish
  - *****Request*****
  ```
  [
    {
      "topic": "topic A",
      "event_id": "event_id A",
      "timestamp": "timestamp",
      "source": "source A",
      "payload": {"key": "value"}
    },
    {
      "topic": "topic A",
      "event_id": "event_id A",
      "timestamp": "timestamp",
      "source": "source A",
      "payload": {"key": "value"}
    }
  ]
  ```
  atau bisa juga
  ```
  {
    "topic": "topic A",
    "event_id": "event_id A",
    "timestamp": "timestamp",
    "source": "source A",
    "payload": {"key": "value"}
  }
  ```

  - *****Response*****
  ```
  {
    "status": "ok",
    "processed": [
      "event_id A",
      "event_id B"
    ],
    "duplicates": 0,
    "total_received": 2
  }
  ```


- ### /events?topic=xxxxx
  - *****Response*****
  ```
  [
    {
      "topic": "topic A",
      "event_id": "event_id A",
      "timestamp": "timestamp",
      "source": "source A",
      "payload": {"key": "value"}
    },
    {
      "topic": "topic A",
      "event_id": "event_id A",
      "timestamp": "timestamp",
      "source": "source A",
      "payload": {"key": "value"}
    }
  ]
  ```

- ### /stats
  - *****Response*****
  ```
  {
    "received": 2,
    "unique_processed": 2,
    "duplicate_dropped": 0,
    "topics": [
      "sensor",
      "sensor-humidity",
      "sensor-temp",
      "system-log",
      "user-activity"
    ],
    "uptime": "0:06:54"
  }
  ```

---


---
