# SBP Project - MongoDB + Metabase Performance Analysis

## 🚀 Quick Start

**Za brzi start (3 koraka):**
```powershell
# 1. Pokreni MongoDB
net start MongoDB

# 2. Pokreni Metabase
docker compose up -d

# 3. Kreiraj Dashboard
cd v2\scripts
python metabase_integration.py
```

**Dashboard URL**: http://localhost:3000/dashboard/40

---

## 📚 Dokumentacija

| Dokument | Opis | Kada koristiti |
|----------|------|----------------|
| **[FINALNO_RESENJE.md](FINALNO_RESENJE.md)** | ✅ **POČNI OVDE** - Kompletan pregled | Prvo čitaj ovo |
| **[QUICK_START.md](QUICK_START.md)** | Brzi start (1 strana) | Brzo pokretanje |
| **[KAKO_DODATI_UPITE.md](KAKO_DODATI_UPITE.md)** | Uputstvo za dashboard | Dodavanje upita |
| **[METABASE_SETUP_FIXED.md](METABASE_SETUP_FIXED.md)** | Docker networking | Docker problemi |
| **[NETWORK_DIAGRAM.md](NETWORK_DIAGRAM.md)** | Vizuelni dijagrami | Razumevanje problema |
| **[METABASE_QUERIES_FIXED.md](METABASE_QUERIES_FIXED.md)** | Detalji o upitima | Struktura upita |

---

## 🎯 Rešeni Problemi

### ✅ Problem 1: Docker Networking
- **Uzrok**: Metabase (Docker) nije mogao da se poveže sa MongoDB (lokalno)
- **Rešenje**: `host.docker.internal` umesto `localhost`

### ✅ Problem 2: Pogrešna Struktura Upita
- **Uzrok**: Upiti koristili flat polja umesto nested strukture
- **Rešenje**: Ispravljeni upiti da koriste `financial.budget`, `ratings.vote_average`, itd.

### ✅ Problem 3: UTF-8 Encoding
- **Uzrok**: Windows konzola ne podržava ćirilične karaktere
- **Rešenje**: Eksplicitno postavljanje UTF-8 encoding-a

---

## 📊 Kreirani Upiti

| # | Naziv | Rezultata | Status |
|---|-------|-----------|--------|
| 1 | Top 10 Profitable Companies | 10 | ✅ |
| 2 | Ratings by Genre and Decade | 20 | ✅ |
| 3 | Movies by Month | 12 | ✅ |
| 4 | Genre Pairs Revenue | 10 | ✅ |
| 5 | Runtime by Country | 15 | ✅ |

---

## 🧪 Test Skripte

```powershell
cd v2\scripts

# Test konekcije
python test_connection.py

# Test upita
python test_fixed_queries.py

# Provera strukture
python check_collection.py
```

---

## 📁 Dataset

**TMDB Movies Dataset** (930k movies)  
[Kaggle Link](https://www.kaggle.com/datasets/asaniczka/tmdb-movies-dataset-2023-930k-movies)

---

## 🎓 Tehnologije

- **MongoDB** 7.x - NoSQL baza podataka (1,281,525 dokumenata)
- **Metabase** (Docker) - BI i Analytics platforma
- **Python** 3.x - Skripting i automatizacija
- **Docker** - Kontejnerizacija

---

## ✅ Status: REŠENO!

Sve je testirano i radi:
- ✅ MongoDB konekcija
- ✅ Metabase integracija
- ✅ 5 upita kreiranih
- ✅ Dashboard spreman
- ✅ Dokumentacija kompletna

**Za pomoć, pogledaj: [FINALNO_RESENJE.md](FINALNO_RESENJE.md)**
