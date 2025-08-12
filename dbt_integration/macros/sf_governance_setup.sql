{% macro sf_governance_setup() %}
  {%- if execute -%}
    -- Setup required reference data for DBT integration with SF Gov Platform

    -- 1) Default system for dbt (matches DIM_SYSTEM columns)
    MERGE INTO GOV_PLATFORM.CATALOG.DIM_SYSTEM AS target
    USING (
        SELECT 
            'DBT_{{ target.name | upper }}' AS SYSTEM_ID,
            'dbt ({{ target.name }})'       AS SYSTEM_NAME,
            'ANALYTICS_TOOL'                AS SYSTEM_TYPE,
            'DATA_ENGINEERING'              AS OWNER_GROUP,
            'dbt transformation engine for {{ target.name }} environment' AS DESCRIPTION,
            CURRENT_TIMESTAMP()             AS CREATED_AT,
            CURRENT_TIMESTAMP()             AS UPDATED_AT
    ) AS source
    ON target.SYSTEM_ID = source.SYSTEM_ID
    WHEN NOT MATCHED THEN INSERT (
        SYSTEM_ID, SYSTEM_NAME, SYSTEM_TYPE, OWNER_GROUP, DESCRIPTION, CREATED_AT, UPDATED_AT
    ) VALUES (
        source.SYSTEM_ID, source.SYSTEM_NAME, source.SYSTEM_TYPE, source.OWNER_GROUP, source.DESCRIPTION, source.CREATED_AT, source.UPDATED_AT
    );

    -- 2) Default domain (matches DIM_DOMAIN columns)
    MERGE INTO GOV_PLATFORM.CATALOG.DIM_DOMAIN AS target
    USING (
        SELECT 
            'ANALYTICS' AS DOMAIN_ID,
            'Analytics' AS DOMAIN_NAME,
            'Analytics and data transformation domain for dbt models and derived datasets' AS DESCRIPTION,
            'HIGH' AS CRITICALITY,
            'DATA_ENGINEERING' AS OWNER_GROUP,
            CURRENT_TIMESTAMP() AS CREATED_AT,
            CURRENT_TIMESTAMP() AS UPDATED_AT
    ) AS source
    ON target.DOMAIN_ID = source.DOMAIN_ID
    WHEN NOT MATCHED THEN INSERT (
        DOMAIN_ID, DOMAIN_NAME, DESCRIPTION, CRITICALITY, OWNER_GROUP, CREATED_AT, UPDATED_AT
    ) VALUES (
        source.DOMAIN_ID, source.DOMAIN_NAME, source.DESCRIPTION, source.CRITICALITY, source.OWNER_GROUP, source.CREATED_AT, source.UPDATED_AT
    );

    -- 3) Default party (matches OWNERSHIP.DIM_PARTY columns)
    MERGE INTO GOV_PLATFORM.OWNERSHIP.DIM_PARTY AS target
    USING (
        SELECT 
            'DBT_SYSTEM'    AS PARTY_ID,
            'GROUP'         AS PARTY_TYPE,
            'DBT System'    AS PARTY_NAME,
            NULL            AS EMAIL,
            NULL            AS MANAGER_PARTY_ID,
            CURRENT_TIMESTAMP() AS CREATED_AT,
            CURRENT_TIMESTAMP() AS UPDATED_AT
    ) AS source
    ON target.PARTY_ID = source.PARTY_ID
    WHEN NOT MATCHED THEN INSERT (
        PARTY_ID, PARTY_TYPE, PARTY_NAME, EMAIL, MANAGER_PARTY_ID, CREATED_AT, UPDATED_AT
    ) VALUES (
        source.PARTY_ID, source.PARTY_TYPE, source.PARTY_NAME, source.EMAIL, source.MANAGER_PARTY_ID, source.CREATED_AT, source.UPDATED_AT
    );

    -- 4) Classification vocab (CLASSIFICATION columns)
    MERGE INTO GOV_PLATFORM.GOVERNANCE.CLASSIFICATION AS target
    USING (
        SELECT * FROM VALUES
            ('INTERNAL',     'Internal',     'Internal company data with standard access controls'),
            ('CONFIDENTIAL', 'Confidential', 'Confidential business data requiring restricted access'),
            ('PII',          'PII',          'Data containing personal identifiers requiring privacy protection'),
            ('PUBLIC',       'Public',       'Data approved for public consumption')
    ) AS source(CLASS_ID, CLASS_NAME, DESCRIPTION)
    ON target.CLASS_ID = source.CLASS_ID
    WHEN NOT MATCHED THEN INSERT (
        CLASS_ID, CLASS_NAME, DESCRIPTION, CREATED_AT, UPDATED_AT
    ) VALUES (
        source.CLASS_ID, source.CLASS_NAME, source.DESCRIPTION, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()
    );

    -- 5) Generic DQ rule (matches DQ_RULE columns)
    MERGE INTO GOV_PLATFORM.QUALITY.DQ_RULE AS target
    USING (
        SELECT 
            'row_count_check' AS RULE_ID,
            'Row Count Validation' AS RULE_NAME,
            'COMPLETENESS' AS RULE_TYPE,
            NULL AS DATASET_ID,
            NULL AS COLUMN_NAME,
            OBJECT_CONSTRUCT('min_rows', 0, 'description', 'Validates table has data') AS CONFIG_JSON,
            'LOW' AS SEVERITY,
            'DBT_SYSTEM' AS OWNER_PARTY_ID,
            TRUE AS ENABLED_FLAG,
            CURRENT_TIMESTAMP() AS CREATED_AT,
            CURRENT_TIMESTAMP() AS UPDATED_AT
    ) AS source
    ON target.RULE_ID = source.RULE_ID
    WHEN NOT MATCHED THEN INSERT (
        RULE_ID, RULE_NAME, RULE_TYPE, DATASET_ID, COLUMN_NAME, CONFIG_JSON, SEVERITY, OWNER_PARTY_ID, ENABLED_FLAG, CREATED_AT, UPDATED_AT
    ) VALUES (
        source.RULE_ID, source.RULE_NAME, source.RULE_TYPE, source.DATASET_ID, source.COLUMN_NAME, source.CONFIG_JSON, source.SEVERITY, source.OWNER_PARTY_ID, source.ENABLED_FLAG, source.CREATED_AT, source.UPDATED_AT
    );

    {{ log("SF Gov Platform reference data setup completed", info=True) }}
  {%- endif -%}
{% endmacro %}
