## Pipeline Data Engineering (Débutant): CSV -> PostgreSQL avec Python

Ce projet propose un pipeline simple et débutant pour ingérer un fichier CSV, effectuer un nettoyage basique, puis charger les données dans PostgreSQL.

### 1) Prérequis
- Python 3.10+
- PostgreSQL accessible (local ou distant)
- Un fichier CSV encodé en UTF-8

### 2) Installation
1. Créez et activez un environnement virtuel (Windows PowerShell):
```powershell
python -m venv env
./env/Scripts/Activate.ps1
```
2. Installez les dépendances:
```powershell
pip install -r reqirements.txt
```

### 3) Configuration
1. Copiez le fichier `.env.example` en `.env` et remplissez vos informations PostgreSQL:
```env
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=my_database
POSTGRES_USER=my_user
POSTGRES_PASSWORD=my_password
CSV_PATH=./data/input.csv
```
2. Placez votre fichier CSV à l'emplacement voulu (par défaut `./data/input.csv`). Créez le dossier `data/` si besoin.

### 4) Exécution manuelle
```powershell
python pipeline.py --csv "./data/mon_fichier.csv" --table "csv_data"
```
- Sans arguments, le script utilisera `CSV_PATH` depuis `.env` et `csv_data` comme nom de table.

### 5) Ce que fait le pipeline
- Lit le CSV avec `pandas`
- Nettoie simplement: trim des espaces, remplace les valeurs vides évidentes, supprime les doublons
- Crée la table si elle n'existe pas (schéma basique inféré par `pandas`)
- Insère les lignes dans PostgreSQL

### 6) Automatisation (Windows - Task Scheduler)
1. Ouvrez "Planificateur de tâches".
2. Créez une "Tâche de base...".
3. Définissez la planification (quotidienne, horaire, etc.).
4. Action: "Démarrer un programme".
   - Programme/script: `powershell.exe`
   - Ajouter des arguments: `-NoProfile -ExecutionPolicy Bypass -Command "& { Set-Location \"C:\\Users\\USER\\Desktop\\Projet\\Data-engenieer-pipeline\"; ./env/Scripts/Activate.ps1; python .\pipeline.py --csv \"C:\\chemin\\vers\\votre.csv\" --table csv_data }"`
   - Démarrer dans (facultatif): votre dossier du projet
5. Cochez "Exécuter même si l'utilisateur n'est pas connecté" si souhaité.

Alternative (script .bat simple):
```bat
@echo off
cd /d C:\Users\USER\Desktop\Projet\Data-engenieer-pipeline
call .\env\Scripts\activate.bat
python pipeline.py --csv .\data\input.csv --table csv_data
```
Planifiez ce `.bat` avec le Planificateur de tâches.

### 7) Dépannage
- Erreur de connexion: vérifiez `POSTGRES_*` dans `.env` et que PostgreSQL accepte les connexions.
- Erreurs d'encodage: assurez-vous que le CSV est en UTF-8.
- Types de colonnes: ce pipeline infère les types. Pour des schémas stricts, définissez les `dtype` dans `pandas` ou créez la table avec un SQL explicite.

### 8) Structure simple
```
Data-engenieer-pipeline/
  pipeline.py
  .env.example
  reqirements.txt
  README.md
  env/            # venv (créé par vous)
  data/           # vos CSV (à créer)
```


