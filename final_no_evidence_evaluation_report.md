# Multilingual Text-to-SQL Baseline Report (Without Evidence)

*(Consolidated baseline Execution Accuracy (EX) parsed directly from raw evaluation logs. Exact Match (EM) is omitted.)*

## 🌐 Overall Performance across All Databases

Averages are calculated over all 3 databases combined. Languages are columns, models are rows.

### Foreign/Non-Indic Languages

| Model | English | Arabic | Spanish | Korean | Italian | French | Chinese | Average EX |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| DeepSeek v3.2 | 67.2% | 60.3% | 70.0% | 61.4% | 69.9% | 71.8% | 53.8% | **64.9%** |
| Llama 3.3 70B | 55.7% | 44.5% | 53.6% | 44.4% | 55.3% | 52.8% | 46.4% | **50.4%** |
| MiniMax m2.7 | 58.3% | 50.3% | 55.5% | 53.5% | 59.5% | 59.3% | 43.4% | **54.2%** |
| Qwen3 8B | 49.2% | 45.7% | 46.9% | 50.0% | 51.0% | 54.5% | 39.7% | **48.1%** |
| **Average EX (per language)** | **57.6%** | **50.2%** | **56.5%** | **52.3%** | **58.9%** | **59.6%** | **45.8%** | **54.4%** |

### Indic Languages

| Model | Hindi | Bengali | Tamil | Telugu | Marathi | Hinglish | Average EX |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| DeepSeek v3.2 | 58.3% | 56.9% | 53.0% | 56.8% | 54.6% | 64.9% | **57.4%** |
| Llama 3.3 70B | 44.8% | 40.8% | 40.2% | 31.7% | 36.6% | 40.1% | **39.0%** |
| MiniMax m2.7 | 53.8% | 46.4% | 48.3% | 44.6% | 48.6% | 53.2% | **49.1%** |
| Qwen3 8B | 29.8% | 27.2% | 21.8% | 24.2% | 22.9% | 40.4% | **27.7%** |
| **Average EX (per language)** | **46.7%** | **42.8%** | **40.8%** | **39.3%** | **40.7%** | **49.7%** | **43.3%** |


---

## 📊 Database: INDIA Economic Census Firms

### Foreign/Non-Indic Languages

| Model | English | Arabic | Spanish | Korean | Italian | French | Chinese | Average EX |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| DeepSeek v3.2 | 74.0% | 66.0% | 73.0% | 69.0% | 72.0% | 75.0% | 46.0% | **67.9%** |
| Llama 3.3 70B | 65.0% | 41.0% | 57.0% | 51.0% | 57.0% | 58.0% | 43.0% | **53.1%** |
| MiniMax m2.7 | 66.0% | 53.0% | 54.0% | 51.0% | 61.0% | 61.0% | 35.0% | **54.4%** |
| Qwen3 8B | 30.0% | 43.0% | 45.0% | 46.0% | 49.0% | 54.0% | 22.0% | **41.3%** |
| **Average EX (per language)** | **58.8%** | **50.8%** | **57.2%** | **54.2%** | **59.8%** | **62.0%** | **36.5%** | **54.2%** |

### Indic Languages

| Model | Hindi | Bengali | Tamil | Telugu | Marathi | Hinglish | Average EX |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| DeepSeek v3.2 | 67.0% | 59.0% | 61.0% | 67.0% | 60.0% | 68.0% | **63.7%** |
| Llama 3.3 70B | 56.0% | 37.0% | 58.0% | 36.0% | 41.0% | 31.0% | **43.2%** |
| MiniMax m2.7 | 54.0% | 48.0% | 53.0% | 51.0% | 53.0% | 63.0% | **53.7%** |
| Qwen3 8B | 19.0% | 21.0% | 20.0% | 15.0% | 15.0% | 17.0% | **17.8%** |
| **Average EX (per language)** | **49.0%** | **41.2%** | **48.0%** | **42.2%** | **42.2%** | **44.8%** | **44.6%** |


---

## 📊 Database: INDIA ICRISAT District Level Agricultural Data

### Foreign/Non-Indic Languages

| Model | English | Arabic | Spanish | Korean | Italian | French | Chinese | Average EX |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| DeepSeek v3.2 | 51.4% | 53.0% | 56.0% | 47.0% | 60.0% | 58.0% | 47.0% | **53.2%** |
| Llama 3.3 70B | 48.0% | 37.0% | 45.0% | 33.0% | 47.0% | 48.0% | 39.0% | **42.4%** |
| MiniMax m2.7 | 36.0% | 36.0% | 41.0% | 38.0% | 46.0% | 47.0% | 38.0% | **40.3%** |
| Qwen3 8B | 51.0% | 45.0% | 45.0% | N/A | 50.0% | 46.0% | 43.0% | **46.7%** |
| **Average EX (per language)** | **46.6%** | **42.8%** | **46.8%** | **39.3%** | **50.8%** | **49.8%** | **41.8%** | **45.6%** |

### Indic Languages

| Model | Hindi | Bengali | Tamil | Telugu | Marathi | Hinglish | Average EX |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| DeepSeek v3.2 | 52.2% | 43.5% | 36.2% | 49.3% | 35.5% | 52.2% | **44.8%** |
| Llama 3.3 70B | 37.0% | 33.0% | 23.0% | 29.0% | 18.0% | 48.0% | **31.3%** |
| MiniMax m2.7 | 39.0% | 34.0% | 30.0% | 32.0% | 26.0% | 33.0% | **32.3%** |
| Qwen3 8B | 34.0% | 24.0% | 20.0% | 26.0% | 14.0% | 44.0% | **27.0%** |
| **Average EX (per language)** | **40.5%** | **33.6%** | **27.3%** | **34.1%** | **23.4%** | **44.3%** | **33.9%** |


---

## 📊 Database: INDIA PRIMARY POPULATION CENSUS 1991

### Foreign/Non-Indic Languages

| Model | English | Arabic | Spanish | Korean | Italian | French | Chinese | Average EX |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| DeepSeek v3.2 | 76.2% | 61.9% | 81.0% | 68.3% | 77.8% | 82.5% | 68.3% | **73.7%** |
| Llama 3.3 70B | 54.0% | 55.6% | 58.7% | 49.2% | 61.9% | 52.4% | 57.1% | **55.6%** |
| MiniMax m2.7 | 73.0% | 61.9% | 71.4% | 71.4% | 71.4% | 69.8% | 57.1% | **68.0%** |
| Qwen3 8B | 66.7% | 49.2% | 50.8% | 54.0% | 54.0% | 63.5% | 54.0% | **56.0%** |
| **Average EX (per language)** | **67.5%** | **57.1%** | **65.5%** | **60.7%** | **66.3%** | **67.1%** | **59.1%** | **63.3%** |

### Indic Languages

| Model | Hindi | Bengali | Tamil | Telugu | Marathi | Hinglish | Average EX |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| DeepSeek v3.2 | 55.6% | 68.2% | 61.9% | 54.0% | 68.2% | 74.6% | **63.8%** |
| Llama 3.3 70B | 41.3% | 52.4% | 39.7% | 30.2% | 50.8% | 41.3% | **42.6%** |
| MiniMax m2.7 | 68.3% | 57.1% | 61.9% | 50.8% | 66.7% | 63.5% | **61.4%** |
| Qwen3 8B | 36.5% | 36.5% | 25.4% | 31.8% | 39.7% | 60.3% | **38.4%** |
| **Average EX (per language)** | **50.4%** | **53.6%** | **47.2%** | **41.7%** | **56.4%** | **59.9%** | **51.5%** |


---

