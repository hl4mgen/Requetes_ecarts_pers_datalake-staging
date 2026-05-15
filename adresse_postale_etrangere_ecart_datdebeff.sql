-- Requête pour le Rapport de Synthèse par table (Étape 1)
WITH
    -- 1. Sélection de la dernière situation connue en staging (TSAC) - Adresses françaises uniquement
    last_situation_staging AS (
        SELECT
            CAST(num_personne AS VARCHAR) AS num_personne,
            date_deb_effet AS date_effet_staging
        FROM (
            SELECT
                num_personne,
                date_deb_effet,
                ROW_NUMBER() OVER (PARTITION BY num_personne ORDER BY date_deb_effet DESC, num_evenement DESC, idfichier DESC) AS rn
            FROM tsac.ldco_personne_adresse
            WHERE
                code_etat = 'VA'
                AND pays = 'FRA'
        ) AS subquery_staging
        WHERE
            rn = 1
    ),
    -- 2. Sélection de la dernière situation connue dans TLAK_PER
    last_situation_tlak AS (
        SELECT
            CAST(refsrcper AS VARCHAR) AS num_personne,
            datdebeffutladr AS date_effet_tlak
        FROM (
            SELECT
                refsrcper,
                datdebeffutladr,
                ROW_NUMBER() OVER (PARTITION BY refsrcper ORDER BY datdebeffutladr DESC) AS rn
            FROM tlak_per.lak_pers_adresse_postale_francaise
            WHERE
                CURRENT_DATE BETWEEN datdebvld AND datfinvld
                AND idrefapl = 'RC-CTR'
        ) AS subquery_tlak
        WHERE
            rn = 1
    ),
    -- 3. Comparaison des deux situations et identification des écarts (détail)
    detailed_comparison AS (
        SELECT
            COALESCE(ls_s.num_personne, ls_t.num_personne) AS identifiant_metier,
            ls_s.date_effet_staging,
            ls_t.date_effet_tlak,
            CASE
                WHEN ls_s.date_effet_staging IS NULL THEN 'Manquant en Staging'
                WHEN ls_t.date_effet_tlak IS NULL THEN 'Manquant en TLAK'
                WHEN ls_s.date_effet_staging <> ls_t.date_effet_tlak THEN 'Date différente'
                ELSE 'Conforme'
            END AS type_ecart
        FROM
            last_situation_staging ls_s
        FULL OUTER JOIN
            last_situation_tlak ls_t ON ls_s.num_personne = ls_t.num_personne
    )
-- 4. Agrégation pour le rapport de synthèse
SELECT
    'TLAK_PER' AS Schema_Name,
    'lak_pers_adresse_postale_francaise' AS Table_Name,
    COUNT(DISTINCT identifiant_metier) AS Nombre_total_lignes_derniere_situation_connue,
    COUNT(CASE WHEN type_ecart <> 'Conforme' THEN 1 END) AS Nombre_lignes_en_ecart,
    ROUND(
        (COUNT(CASE WHEN type_ecart <> 'Conforme' THEN 1 END) * 100.0) / NULLIF(COUNT(DISTINCT identifiant_metier), 0),
        2
    ) AS Taux_ecart,
    'Date de début d''effet' AS Criteres_comparaison
FROM
    detailed_comparison;
