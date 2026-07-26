# Multilingual Text-to-SQL Baseline Report (With Evidence)

*(Consolidated baseline Execution Accuracy (EX) parsed directly from raw evaluation logs with DDL schema evidence injected. Exact Match (EM) is omitted. Partially completed/interrupted runs are strictly filtered out and listed as N/A.)*

## 🌐 Overall Performance across All Databases

Averages are calculated over all 3 databases combined. Languages are columns, models are rows.

### Foreign/Non-Indic Languages

| Model | English | Arabic | Spanish | Korean | Italian | French | Chinese | Average EX |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| DeepSeek v3.2 | 74.3% | 63.7% | 67.1% | 66.9% | 66.7% | 67.4% | 60.2% | **66.6%** |
| Llama 3.3 70B | 61.3% | 62.6% | 69.6% | 62.8% | 67.5% | 68.8% | 63.1% | **65.1%** |
| MiniMax m2.7 | 56.1% | 59.7% | 60.0% | 59.5% | 60.4% | 60.0% | 54.7% | **58.6%** |
| Qwen3 8B | 63.1% | 64.2% | 67.3% | 65.0% | 64.7% | 66.2% | 54.7% | **63.6%** |
| **Average EX (per language)** | **63.7%** | **62.6%** | **66.0%** | **63.6%** | **64.8%** | **65.6%** | **58.2%** | **63.5%** |

### Indic Languages

| Model | Hindi | Bengali | Tamil | Telugu | Marathi | Hinglish | Average EX |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| DeepSeek v3.2 | 67.5% | 64.2% | 64.5% | 63.5% | 63.0% | 69.8% | **65.4%** |
| Llama 3.3 70B | 52.4% | 43.1% | 45.0% | 39.0% | 40.6% | 55.3% | **45.9%** |
| MiniMax m2.7 | 55.9% | 54.1% | 50.1% | 50.0% | 49.5% | 57.0% | **52.8%** |
| Qwen3 8B | 31.5% | 41.6% | 41.7% | 34.8% | 34.6% | 43.8% | **38.0%** |
| **Average EX (per language)** | **51.8%** | **50.7%** | **50.3%** | **46.8%** | **46.9%** | **56.5%** | **50.5%** |


---

## 📊 Database: INDIA Economic Census Firms

### Foreign/Non-Indic Languages

| Model | English | Arabic | Spanish | Korean | Italian | French | Chinese | Average EX |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| DeepSeek v3.2 | 74.0% | 67.0% | 72.0% | 73.0% | 72.0% | 72.0% | 57.0% | **69.6%** |
| Llama 3.3 70B | N/A | 57.0% | 63.0% | 59.0% | 62.0% | 63.0% | 58.0% | **60.3%** |
| MiniMax m2.7 | N/A | 63.0% | 64.0% | 58.0% | 66.0% | 63.0% | 46.0% | **60.0%** |
| Qwen3 8B | 67.0% | 57.0% | 60.0% | 57.0% | 58.0% | 61.0% | 38.0% | **56.9%** |
| **Average EX (per language)** | **70.5%** | **61.0%** | **64.8%** | **61.8%** | **64.5%** | **64.8%** | **49.8%** | **61.7%** |

### Indic Languages

| Model | Hindi | Bengali | Tamil | Telugu | Marathi | Hinglish | Average EX |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| DeepSeek v3.2 | 76.3% | 63.8% | 72.9% | 69.1% | 62.1% | 72.9% | **69.5%** |
| Llama 3.3 70B | 67.8% | 43.1% | 59.3% | 54.5% | 43.1% | 57.6% | **54.2%** |
| MiniMax m2.7 | 76.3% | 62.1% | 67.8% | 65.5% | 55.2% | 71.2% | **66.3%** |
| Qwen3 8B | 5.1% | 39.7% | 52.5% | 34.5% | 25.9% | 25.4% | **30.5%** |
| **Average EX (per language)** | **56.4%** | **52.2%** | **63.1%** | **55.9%** | **46.6%** | **56.8%** | **55.1%** |


---

## 📊 Database: INDIA ICRISAT District Level Agricultural Data

### Foreign/Non-Indic Languages

| Model | English | Arabic | Spanish | Korean | Italian | French | Chinese | Average EX |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| DeepSeek v3.2 | N/A | 48.0% | 53.0% | 50.0% | 52.0% | 54.0% | 49.0% | **51.0%** |
| Llama 3.3 70B | 48.0% | N/A | N/A | N/A | N/A | N/A | N/A | **48.0%** |
| MiniMax m2.7 | 36.0% | 43.0% | 51.0% | 46.0% | 47.0% | 44.0% | 45.0% | **44.6%** |
| Qwen3 8B | 51.0% | N/A | N/A | N/A | N/A | N/A | N/A | **51.0%** |
| **Average EX (per language)** | **45.0%** | **45.5%** | **52.0%** | **48.0%** | **49.5%** | **49.0%** | **47.0%** | **48.6%** |

### Indic Languages

| Model | Hindi | Bengali | Tamil | Telugu | Marathi | Hinglish | Average EX |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| DeepSeek v3.2 | N/A | N/A | N/A | N/A | N/A | N/A | **0.0%** |
| Llama 3.3 70B | 37.0% | 33.0% | 23.0% | 29.0% | 18.0% | 48.0% | **31.3%** |
| MiniMax m2.7 | 39.0% | 34.0% | 30.0% | 32.0% | 26.0% | 33.0% | **32.3%** |
| Qwen3 8B | 34.0% | 24.0% | 20.0% | 26.0% | 14.0% | 44.0% | **27.0%** |
| **Average EX (per language)** | **36.7%** | **30.3%** | **24.3%** | **29.0%** | **19.3%** | **41.7%** | **22.7%** |


---

## 📊 Database: INDIA PRIMARY POPULATION CENSUS 1991

### Foreign/Non-Indic Languages

| Model | English | Arabic | Spanish | Korean | Italian | French | Chinese | Average EX |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| DeepSeek v3.2 | 74.6% | 76.2% | 76.2% | 77.8% | 76.2% | 76.2% | 74.6% | **76.0%** |
| Llama 3.3 70B | 74.6% | 68.3% | 76.2% | 66.7% | 73.0% | 74.6% | 68.3% | **71.7%** |
| MiniMax m2.7 | 76.2% | 73.0% | 65.1% | 74.6% | 68.3% | 73.0% | 73.0% | **71.9%** |
| Qwen3 8B | 71.4% | 71.4% | 74.6% | 73.0% | 71.4% | 71.4% | 71.4% | **72.1%** |
| **Average EX (per language)** | **74.2%** | **72.2%** | **73.0%** | **73.0%** | **72.2%** | **73.8%** | **71.8%** | **72.9%** |

### Indic Languages

| Model | Hindi | Bengali | Tamil | Telugu | Marathi | Hinglish | Average EX |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| DeepSeek v3.2 | 58.7% | 64.5% | 56.1% | 57.9% | 63.9% | 66.7% | **61.3%** |
| Llama 3.3 70B | 52.4% | 53.2% | 52.6% | 33.3% | 60.7% | 60.3% | **52.1%** |
| MiniMax m2.7 | 52.4% | 66.1% | 52.6% | 52.6% | 67.2% | 66.7% | **59.6%** |
| Qwen3 8B | 55.6% | 61.3% | 52.6% | 43.9% | 63.9% | 61.9% | **56.5%** |
| **Average EX (per language)** | **54.8%** | **61.3%** | **53.5%** | **46.9%** | **63.9%** | **63.9%** | **57.4%** |


---

