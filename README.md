# Mini ETL Data Lake Pipeline

Ovaj projekat implementira jednostavan **ETL pipeline** koristeći `Python`, sa tri sloja obrade podataka:

- **Bronze**: učitavanje i validacija sirovih CSV fajlova,
- **Silver**: obrada i transformacije podataka (merge i izračunavanja),
- **Gold**: agregacije za analitiku.

## Kako pokrenuti projekat

Da biste pokrenuli ETL pipeline i generisali vizualizacije, pratite sledeće korake:

### 1. Kloniranje repozitorijuma

```bash
git clone https://github.com/janko3/data_etl.git
cd data_etl
```

2. Kreiranje i aktivacija virtuelnog okruženja
   Windows:

```
python -m venv venv
venv\Scripts\activate
```

3. Instalacija zavisnosti

```
pip install -r requirements.txt
```

5. Dodavanje ulaznih podataka

```
U folder raw/ ubacite sledeće CSV fajlove:

employees.csv
salaries.csv
departments.csv
```

6. Pokretanje pipeline-a

```
python run_pipeline.py

```
