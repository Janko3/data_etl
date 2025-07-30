# Mini ETL Data Lake Pipeline

Ovaj projekat implementira jednostavan **ETL pipeline** koristeći `Python`, sa tri sloja obrade podataka:

- **Bronze**: učitavanje i validacija sirovih CSV fajlova,
- **Silver**: obrada i transformacije podataka (merge i izračunavanja),
- **Gold**: agregacije za analitiku.
