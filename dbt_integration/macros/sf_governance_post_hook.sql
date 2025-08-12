{% macro sf_governance_post_hook() %}
  {%- if execute -%}
    -- Sync dbt model metadata to GOV_PLATFORM

    {% set model_database = this.database %}
    {% set model_schema   = this.schema %}
    {% set model_table    = this.identifier %}
    {% set full_model_name = model_database ~ '.' ~ model_schema ~ '.' ~ model_table %}

    -- Derive materialization safely from model.config
    {% set materialized = (model.config.get('materialized') | upper) if model.config.get('materialized') else 'VIEW' %}

    -- 1) Upsert dataset
    MERGE INTO GOV_PLATFORM.CATALOG.DIM_DATASET AS target
    USING (
        SELECT 
            'dbt_' || '{{ model_database }}' || '_' || '{{ model_schema }}' || '_' || '{{ model_table }}' AS DATASET_ID,
            '{{ full_model_name }}' AS CATALOG_NAME,
            'DBT_{{ target.name | upper }}' AS SYSTEM_ID,
            coalesce('{{ model.config.get("meta", {}).get("domain", "") }}','ANALYTICS') AS DOMAIN_ID,
            'SNOWFLAKE' AS PLATFORM,
            '{{ model_database }}' AS DATABASE_NAME,
            '{{ model_schema }}' AS SCHEMA_NAME,
            '{{ model_table }}' AS OBJECT_NAME,
            '{{ materialized }}' AS OBJECT_TYPE,
            CASE 
                WHEN UPPER('{{ model_schema }}') LIKE '%RAW%' OR UPPER('{{ model_schema }}') LIKE '%BRONZE%' THEN 'BRONZE'
                WHEN UPPER('{{ model_schema }}') LIKE '%STAGING%' OR UPPER('{{ model_schema }}') LIKE '%SILVER%' THEN 'SILVER'
                WHEN UPPER('{{ model_schema }}') LIKE '%MART%' OR UPPER('{{ model_schema }}') LIKE '%GOLD%' THEN 'GOLD'
                ELSE 'SILVER'
            END AS DV_LAYER,
            CASE 
                WHEN '{{ model.config.get("meta", {}).get("classification", "") }}' != '' 
                THEN UPPER('{{ model.config.get("meta", {}).get("classification", "") }}')
                ELSE 'INTERNAL'
            END AS CLASSIFICATION,
            {% if model.config.get("meta", {}).get("contains_pii", false) %}TRUE{% else %}FALSE{% endif %} AS IS_CDE,
            '{{ (model.description | replace("'", "''")) if model.description else ("dbt model " ~ model_table) }}' AS DESCRIPTION,
            NULL AS RETENTION_POLICY_ID,
            'DRAFT' AS CERTIFICATION,
            CURRENT_TIMESTAMP() AS CREATED_AT,
            CURRENT_TIMESTAMP() AS UPDATED_AT
    ) AS source
    ON target.DATASET_ID = source.DATASET_ID
    WHEN MATCHED THEN UPDATE SET
        CATALOG_NAME   = source.CATALOG_NAME,
        OBJECT_TYPE    = source.OBJECT_TYPE,
        DV_LAYER       = source.DV_LAYER,
        CLASSIFICATION = source.CLASSIFICATION,
        IS_CDE         = source.IS_CDE,
        DESCRIPTION    = source.DESCRIPTION,
        UPDATED_AT     = source.UPDATED_AT
    WHEN NOT MATCHED THEN INSERT (
        DATASET_ID, CATALOG_NAME, SYSTEM_ID, DOMAIN_ID, PLATFORM,
        DATABASE_NAME, SCHEMA_NAME, OBJECT_NAME, OBJECT_TYPE, DV_LAYER,
        CLASSIFICATION, IS_CDE, DESCRIPTION, RETENTION_POLICY_ID,
        CERTIFICATION, CREATED_AT, UPDATED_AT
    ) VALUES (
        source.DATASET_ID, source.CATALOG_NAME, source.SYSTEM_ID, source.DOMAIN_ID, source.PLATFORM,
        source.DATABASE_NAME, source.SCHEMA_NAME, source.OBJECT_NAME, source.OBJECT_TYPE, source.DV_LAYER,
        source.CLASSIFICATION, source.IS_CDE, source.DESCRIPTION, source.RETENTION_POLICY_ID,
        source.CERTIFICATION, source.CREATED_AT, source.UPDATED_AT
    );

    {% set dataset_id = 'dbt_' ~ model_database ~ '_' ~ model_schema ~ '_' ~ model_table %}

    -- 2) Lineage node for this dataset
    MERGE INTO GOV_PLATFORM.LINEAGE.LINEAGE_NODE AS target
    USING (
        SELECT 
            '{{ dataset_id }}' AS NODE_ID,
            'DATASET' AS NODE_TYPE,
            '{{ dataset_id }}' AS REF_ID,
            '{{ full_model_name }}' AS NAME,
            '{{ (model.description | replace("'", "''")) if model.description else ("dbt model " ~ model_table) }}' AS DESCRIPTION,
            CURRENT_TIMESTAMP() AS CREATED_AT,
            CURRENT_TIMESTAMP() AS UPDATED_AT
    ) AS source
    ON target.NODE_ID = source.NODE_ID
    WHEN MATCHED THEN UPDATE SET
        NAME = source.NAME,
        DESCRIPTION = source.DESCRIPTION,
        UPDATED_AT = source.UPDATED_AT
    WHEN NOT MATCHED THEN INSERT (NODE_ID, NODE_TYPE, REF_ID, NAME, DESCRIPTION, CREATED_AT, UPDATED_AT)
    VALUES (source.NODE_ID, source.NODE_TYPE, source.REF_ID, source.NAME, source.DESCRIPTION, source.CREATED_AT, source.UPDATED_AT);

    -- 3) Lineage edges for dependencies (limited to model nodes)
    {% if model.depends_on.nodes %}
      {% for dependency in model.depends_on.nodes %}
        {% set dep_parts = dependency.split('.') %}
        {% if dep_parts | length >= 2 %}
          {% set dep_name = dep_parts[-1] %}
          {% set source_node_id = 'dbt_' ~ model_database ~ '_' ~ model_schema ~ '_' ~ dep_name %}
          MERGE INTO GOV_PLATFORM.LINEAGE.LINEAGE_EDGE AS target
          USING (
              SELECT 
                  'edge_' || '{{ source_node_id }}' || '_to_' || '{{ dataset_id }}' AS EDGE_ID,
                  '{{ source_node_id }}' AS SRC_NODE_ID,
                  '{{ dataset_id }}'     AS TGT_NODE_ID,
                  'TRANSFORMS'           AS EDGE_TYPE,
                  'dbt_model_{{ model_table }}' AS LOGIC_REF,
                  TRUE AS ACTIVE_FLAG,
                  CURRENT_TIMESTAMP() AS CREATED_AT,
                  CURRENT_TIMESTAMP() AS UPDATED_AT
          ) AS source
          ON target.EDGE_ID = source.EDGE_ID
          WHEN MATCHED THEN UPDATE SET
              LOGIC_REF = source.LOGIC_REF,
              ACTIVE_FLAG = source.ACTIVE_FLAG,
              UPDATED_AT = source.UPDATED_AT
          WHEN NOT MATCHED THEN INSERT (EDGE_ID, SRC_NODE_ID, TGT_NODE_ID, EDGE_TYPE, LOGIC_REF, ACTIVE_FLAG, CREATED_AT, UPDATED_AT)
          VALUES (source.EDGE_ID, source.SRC_NODE_ID, source.TGT_NODE_ID, source.EDGE_TYPE, source.LOGIC_REF, source.ACTIVE_FLAG, source.CREATED_AT, source.UPDATED_AT);
        {% endif %}
      {% endfor %}
    {% endif %}

    -- 4) Column metadata to MAP_DATASET_ATTRIBUTE
    {% set columns_query %}
        SELECT 
            column_name,
            data_type,
            is_nullable,
            column_default,
            ordinal_position,
            comment
        FROM {{ model_database }}.information_schema.columns 
        WHERE table_schema = '{{ model_schema }}'
          AND table_name   = '{{ model_table }}'
        ORDER BY ordinal_position
    {% endset %}

    {% if execute %}
      {% set results = run_query(columns_query) %}
      {% if results %}
        {% for row in results.rows %}
          MERGE INTO GOV_PLATFORM.CATALOG.MAP_DATASET_ATTRIBUTE AS target
          USING (
              SELECT 
                  'attr_' || '{{ dataset_id }}' || '_' || '{{ row[0] }}' AS MAP_ID,
                  '{{ dataset_id }}' AS DATASET_ID,
                  '{{ row[0] }}'    AS COLUMN_NAME,
                  NULL AS TERM_ID,
                  NULL AS CDE_ID,
                  CASE 
                    WHEN UPPER('{{ row[0] }}') LIKE '%EMAIL%' THEN 'EMAIL'
                    WHEN UPPER('{{ row[0] }}') LIKE '%PHONE%' THEN 'PHONE'
                    WHEN UPPER('{{ row[0] }}') LIKE '%SSN%' OR UPPER('{{ row[0] }}') LIKE '%SOCIAL%' THEN 'SSN'
                    WHEN UPPER('{{ row[0] }}') LIKE '%_ID' OR UPPER('{{ row[0] }}') LIKE 'ID_%' THEN 'IDENTIFIER'
                    WHEN UPPER('{{ row[1] }}') LIKE '%TIMESTAMP%' OR UPPER('{{ row[1] }}') LIKE '%DATE%' THEN 'TEMPORAL'
                    WHEN UPPER('{{ row[1] }}') LIKE '%NUMBER%' OR UPPER('{{ row[1] }}') LIKE '%DECIMAL%' THEN 'NUMERIC'
                    ELSE 'GENERAL'
                  END AS SEMANTIC_TYPE,
                  {% if model.config.get("meta", {}).get("quality_critical_columns", []) %}
                    CASE WHEN '{{ row[0] }}' IN ({{ model.config.get("meta", {}).get("quality_critical_columns", []) | map("string") | join(", ") }}) THEN TRUE ELSE FALSE END
                  {% else %}
                    FALSE
                  {% endif %} AS QUALITY_CRITICAL,
                  COALESCE('{{ row[5] if row[5] else "" }}', '') AS DESCRIPTION,
                  CURRENT_TIMESTAMP() AS CREATED_AT,
                  CURRENT_TIMESTAMP() AS UPDATED_AT
          ) AS source
          ON target.MAP_ID = source.MAP_ID
          WHEN MATCHED THEN UPDATE SET
              SEMANTIC_TYPE   = source.SEMANTIC_TYPE,
              QUALITY_CRITICAL= source.QUALITY_CRITICAL,
              DESCRIPTION     = source.DESCRIPTION,
              UPDATED_AT      = source.UPDATED_AT
          WHEN NOT MATCHED THEN INSERT (MAP_ID, DATASET_ID, COLUMN_NAME, TERM_ID, CDE_ID, SEMANTIC_TYPE, QUALITY_CRITICAL, DESCRIPTION, CREATED_AT, UPDATED_AT)
          VALUES (source.MAP_ID, source.DATASET_ID, source.COLUMN_NAME, source.TERM_ID, source.CDE_ID, source.SEMANTIC_TYPE, source.QUALITY_CRITICAL, source.DESCRIPTION, source.CREATED_AT, source.UPDATED_AT);
        {% endfor %}
      {% endif %}
    {% endif %}

    -- 5) Basic quality metric: row count (once per day per dataset)
    {% set row_count_query %}
      SELECT COUNT(*) AS row_count FROM {{ this }}
    {% endset %}
    {% set row_count_result = run_query(row_count_query) %}
    {% if row_count_result %}
      INSERT INTO GOV_PLATFORM.QUALITY.DQ_RESULT (
        RUN_ID, RULE_ID, DATASET_ID, OUTCOME, METRICS_SUMMARY, EVIDENCE_REF, CREATED_AT, UPDATED_AT
      )
      SELECT 
        'dbt_run_' || TO_VARCHAR(CURRENT_TIMESTAMP(), 'YYYYMMDDHH24MISS') || '_{{ dataset_id }}' AS RUN_ID,
        'row_count_check' AS RULE_ID,
        '{{ dataset_id }}' AS DATASET_ID,
        CASE WHEN {{ row_count_result.rows[0][0] }} >= 0 THEN 'PASS' ELSE 'FAIL' END AS OUTCOME,
        OBJECT_CONSTRUCT('row_count', {{ row_count_result.rows[0][0] }}, 'check_type', 'volume', 'model', '{{ model_table }}')::VARIANT AS METRICS_SUMMARY,
        '{{ full_model_name }}' AS EVIDENCE_REF,
        CURRENT_TIMESTAMP() AS CREATED_AT,
        CURRENT_TIMESTAMP() AS UPDATED_AT
      WHERE NOT EXISTS (
        SELECT 1 FROM GOV_PLATFORM.QUALITY.DQ_RESULT 
        WHERE DATASET_ID = '{{ dataset_id }}'
          AND RULE_ID = 'row_count_check'
          AND DATE(CREATED_AT) = CURRENT_DATE()
      );
    {% endif %}

    {{ log("Governance metadata updated for model: " ~ full_model_name, info=True) }}
  {%- endif -%}
{% endmacro %}
