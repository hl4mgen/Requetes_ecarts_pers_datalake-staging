-- Étape 1 : Contrôle de la table TLAK_PER.personne_physique
-- Comparaison des dates d'effet entre staging (TSAC) et Data Lake (TLAK_PER)

WITH
    -- 1. Sélection de la dernière situation connue en staging (TSAC)
    --    pour chaque personne physique, basée sur date_deb_effet, num_evenement, idfichier
    last_situation_staging AS (
        SELECT
            CAST(num_personne AS VARCHAR) AS num_personne, -- Conversion ajoutée ici
            date_deb_effet AS date_effet_staging
        FROM (
            SELECT
                num_personne,
                date_deb_effet,
                ROW_NUMBER() OVER (PARTITION BY num_personne ORDER BY date_deb_effet DESC, num_evenement DESC, idfichier DESC) AS rn
            FROM tsac.ldco_personne_personne_physique
            WHERE
                code_etat = 'VA' -- Critère de sélection des enregistrements valides en staging
        ) AS subquery_staging
        WHERE
            rn = 1
    ),
    -- 2. Sélection de la dernière situation connue dans TLAK_PER.personne_physique
    --    pour chaque personne physique, basée sur la validité actuelle
    last_situation_tlak AS (
        SELECT
            CAST(refsrcper AS VARCHAR) AS num_personne, -- Conversion ajoutée ici
            datdebeffperphy AS date_effet_tlak
        FROM tlak_per.lak_pers_personne_physique
        WHERE
            CURRENT_DATE BETWEEN datdebvld AND datfinvld -- Critère de sélection de la dernière situation connue (valide à date)
            AND idrefapl = 'RC-CTR' -- Ajout du critère idrefapl si pertinent pour filtrer la source spécifique
    )
-- 3. Comparaison des deux situations et identification des écarts
SELECT
    'TLAK_PER.personne_physique' AS table_concernee,
    COALESCE(ls_s.num_personne, ls_t.num_personne) AS identifiant_metier,
    ls_s.date_effet_staging,
    ls_t.date_effet_tlak,
    CASE
        WHEN ls_s.date_effet_staging IS NULL THEN 'Manquant en Staging'
        WHEN ls_t.date_effet_tlak IS NULL THEN 'Manquant en TLAK'
        WHEN ls_s.date_effet_staging <> ls_t.date_effet_tlak THEN 'Date différente'
        ELSE 'Conforme'
    END AS type_ecart,
    CASE
        WHEN ls_s.date_effet_staging IS NULL THEN 'La personne physique n''existe pas ou n''a pas de situation valide en staging.'
        WHEN ls_t.date_effet_tlak IS NULL THEN 'La personne physique n''existe pas ou n''a pas de situation valide dans TLAK_PER.'
        WHEN ls_s.date_effet_staging <> ls_t.date_effet_tlak THEN 'La date d''effet en staging (' || ls_s.date_effet_staging || ') diffère de celle en TLAK_PER (' || ls_t.date_effet_tlak || ').'
        ELSE 'Les dates d''effet correspondent.'
    END AS commentaire
FROM
    last_situation_staging ls_s
FULL OUTER JOIN
    last_situation_tlak ls_t ON ls_s.num_personne = ls_t.num_personne
WHERE
    ls_s.date_effet_staging IS NULL
    OR ls_t.date_effet_tlak IS NULL
    OR ls_s.date_effet_staging <> ls_t.date_effet_tlak;
