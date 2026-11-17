# Nuctracker v4 — récupération automatique de la production nucléaire réelle

**Objectif** : à chaque lancement, récupérer via RTE les valeurs réelles du parc nucléaire, mettre à jour MongoDB, puis exécuter le calcul et, avec `--publish`, les deux publications Datawrapper. C'est la suite du **kit full v3** : ne plus lancer l'ancien `run_nuctracker.bat`.

## Ce qui a changé

- Nouveau `rte_production_sync.py` : interroge l'API RTE `actual_generation/v1/actual_generations_per_unit` pour les **journées civiles françaises complètes**, pas pour la journée incomplète en cours.
- Gestion du fuseau `Europe/Paris`, y compris le 25 octobre 2026 (**25 heures**) et le 29 mars 2026 (**23 heures**) : plus de décalage `+02:00` fixe ni de découpage des jours en UTC.
- Agrégation temporelle des MW de chaque réacteur ; contrôles de couverture et des puissances ; rejeu des **10 derniers jours** pour les révisions RTE.
- Nouvelle collection : `data.nuclear_production_auto_daily`. Une seule ligne par jour, identifiée par sa date, et mise à jour sans doublons. **La collection historique `data.nuclear_production` n'est jamais modifiée.**
- Le module de prévision privilégie automatiquement la journée issue de la nouvelle collection quand elle existe ; pour l'historique ancien il conserve la lecture de l'ancienne collection.
- Sauvegarde préalable des jours modifiés dans `backups_production_auto/` et possibilité de rétablir uniquement ces jours. Les sauvegardes Datawrapper v3 sont maintenues.
- Blocage de publication si la dernière journée de production disponible date de plus de **3 jours** (modifiable avec `--max-production-lag-days` après accord HES).

## 1. Installation (Windows cmd.exe)

Dans `Documents\Code\Nuctracker-main\Nuctracker-main\nuctracker_pipeline` :

1. Conserver une copie locale intégrale du dossier actuel v3. Ne pas toucher aux scripts historiques `nuctracker_final_pipeline.py`, `nucmonitor.py`, ni à `.env`.
2. Décompresser `HES_Nuctracker_Full_Automation_v4_PRODUCTION_RTE.zip` et **copier le contenu** du sous-dossier `Nuctracker_AUTO_FULL_v4/` dans `nuctracker_pipeline`, en remplaçant les fichiers portant déjà les mêmes noms (notamment `nuctracker_run_full.py` et `run_nuctracker_full.bat`). **Ne pas remplacer le dossier `tests` en bloc : fusionner son contenu**.
3. Installer la base des fuseaux horaires Windows (dans **le Python utilisé pour Nuctracker**):

```cmd
python -m pip install tzdata
python -c "from zoneinfo import ZoneInfo; print(ZoneInfo('Europe/Paris'))"
python -m unittest discover -s tests -v
```

Si le BAT choisit `.venv\Scripts\python.exe`, utiliser de préférence ce Python pour installer `tzdata`, et veiller à ce que les autres dépendances soient aussi présentes dans ce même environnement :

```cmd
.venv\Scripts\python.exe -m pip install tzdata
```

## 2. Premier contrôle RTE de production **sans écriture**

```cmd
python rte_production_sync.py preview
```

Le script demande les accès MongoDB et RTE si les variables d'environnement ne sont pas déjà définies. Il ne modifie **ni** MongoDB **ni** Datawrapper.

Sortie : `outputs_diagnostic\production_rte_YYYYMMDD_....csv` et fichier JSON associé.

À vérifier **avant** le premier enregistrement :
- Une ligne par jour, `Date` (jour français), au moins 40 unités et une puissance totale vraisemblable.
- Dernière journée effectivement disponible chez RTE ; si un jour est incomplet ou si des réacteurs manquent, le script **refuse d'enregistrer**.
- Les jours futurs et la journée actuelle sont exclus ; les jours non publiés en fin de série sont déclarés `RTE_NOT_YET_AVAILABLE`.

Si une erreur se produit, **ne pas aller à `--publish`**. Envoyer le message et le CSV d'audit pour adapter les contrôles au format réel des réponses RTE.

## 3. Première exécution complète **sans Datawrapper**

```cmd
python rte_autosync.py status
python nuctracker_run_full.py
```

La deuxième commande peut écrire dans **deux collections MongoDB** :
- `data.unavs_update` : nouvelles déclarations d'indisponibilité, comme en v3 ;
- `data.nuclear_production_auto_daily` : données réelles quotidiennes, nouvelles ou révisées.

La collection historique `data.nuclear_production` reste intacte. Les CSV de prévision sont produits dans `outputs_full/`, ainsi qu'un contrôle `full_pipeline_qa_...json` dans `outputs_diagnostic/`. **Les graphiques Datawrapper ne sont pas modifiés**.

Rechercher dans le terminal :

```text
[PRODUCTION] Committed ... daily documents to data.nuclear_production_auto_daily
[PRODUCTION] latest actual date: 2026-10-22
[QA] passed; report: ...
[DRY RUN] COMPLETE forecast, graph/table CSVs and QA; Datawrapper NOT MODIFIED
```

**La date `2026-10-22` ci-dessus n'est qu'un exemple ; utiliser la dernière journée vraiment publiée par RTE.**

Ouvrir le dernier `full_pipeline_qa_...json` : il contient `production_auto_sync` (nombre de jours insérés/corrigés), `last_production_date`, les chemins des CSV et le contrôle des disponibilités. Comparer les dernières journées de production et disponibilité avec la source RTE à des dates comparables.

## 4. Après validation des données, publication Datawrapper

```cmd
python nuctracker_run_full.py --publish
```

Le script relance tout : **publications RTE + production réelle RTE + MongoDB + prévision + QA + sauvegarde Datawrapper + 2 republications**.

Le succès doit inclure :

```text
[DW] SUCCESS: graph and table published
[DONE] FULL PIPELINE: RTE unavailability + actual generation + MongoDB + forecast + Datawrapper
```

**Vérifier visuellement** les deux graphiques chez Datawrapper. La v4 n'automatise pas la mise en forme manuelle évoquée dans l'ancien `DATAWRAPPER_GUIDE.md` et ne modifie pas les règles SARIMAX déjà discutées.

## 5. Lancement de routine

Après une exécution v4 vérifiée, le double-clic sur **`run_nuctracker_full.bat`** lance la chaîne complète avec publication. C'est **le nouveau** BAT, remplacé par la version v4 lors de la copie. Désactiver l'ancien `run_nuctracker.bat` et les anciennes tâches planifiées, pour éviter les écritures `EVENT_DATE` en parallèle. Pour un lancement sans saisie, les secrets approuvés HES doivent être disponibles dans l'environnement du compte technique.

```cmd
notepad logs\full_pipeline_latest.log
```

## 6. Si un résultat n'est pas satisfaisant

- **Datawrapper** : sauvegarde de chaque lancement dans `backups_datawrapper/`, via le script existant `restore_datawrapper_full.py`.
- **Production nouvelle collection** : une copie des jours **avant écriture** est créée sous `backups_production_auto/production_auto_before_....json`. Pour revenir uniquement sur les jours de cet import :

```cmd
python restore_production_auto.py --backup-file "backups_production_auto\production_auto_before_YYYYMMDD_HHMMSS_UUUUUU.json"
```

Le script demandera `RESTAURER PRODUCTION AUTO` et bloquera si des modifications ultérieures semblent avoir touché ces jours. La collection historique n'est pas affectée.
- **Indisponibilités MongoDB** : le retour arrière de `data.unavs_update` et du checkpoint `data.nuctracker_sync_state` reste un sujet distinct à gérer avec l'administrateur.

## 7. Limites connues (non modifiées en v4)

1. **SARIMAX** : le code historique efface encore une partie des années récentes lors de la construction des prévisions. Les nouvelles observations réelles sont récupérées et stockées correctement, mais **cela ne corrige pas à lui seul** l'anomalie de prévision identifiée précédemment.
2. Le modèle corrigé de disponibilité présente encore des écarts ponctuels sur certains réacteurs. L'automatisation des importations ne signifie pas validation exhaustive du calcul.
3. **Aucune connexion réelle** à l'API RTE, à MongoDB Atlas ou à Datawrapper n'a été réalisée pour construire ce kit : les tests sont réalisés avec des données simulées et des substituts de base. Le premier `preview`, puis un lancement sans `--publish`, restent indispensables sur ton poste HES.
4. Un historique de production absent depuis plus de 35 jours exige un rattrapage spécifique : ne pas lever ce contrôle sans revue.

**Source RTE :** https://data.rte-france.eu/web/guest/catalog/-/api/doc/user-guide/Actual%2BGeneration/1.1
