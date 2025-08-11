{% macro sf_governance_setup() %}
  {%- if execute -%}
    -- Setup required reference data for DBT integration with SF Gov Platform
    
    -- 1. Ensure default system exists for DBT
    MERGE INTO GOV_PLATFORM.CATALOG.DIM_SYSTEM AS target
    USING (
        SELECT 
            'DBT_{{ target.name | upper }}' AS system_id,
            'dbt ({{ target.name }})' AS system_name,
            'ANALYTICS_TOOL' AS system_type,
            'DATA_ENGINEERING' AS owner_group,
            'dbt transformation engine for {{ target.name }} environment' AS description,
            CURRENT_TIMESTAMP() AS created_at,
            CURRENT_TIMESTAMP() AS updated_at
    ) AS source
    ON target.system_id = source.system_id
    WHEN NOT MATCHED THEN INSERT (
        system_id, system_name, system_type, owner_group, description, created_at, updated_at
    ) VALUES (
        source.system_id, source.system_name, source.system_type, source.owner_group, source.description, source.created_at, source.updated_at
    );

    -- 2. Ensure default domain exists
    MERGE INTO GOV_PLATFORM.CATALOG.DIM_DOMAIN AS target
    USING (
        SELECT 
            'ANALYTICS' AS domain_id,
            'Analytics' AS domain_name,
            'Analytics and data transformation domain for dbt models and derived datasets' AS description,
            'HIGH' AS criticality,
            'DATA_ENGINEERING' AS owner_group,
            CURRENT_TIMESTAMP() AS created_at,
            CURRENT_TIMESTAMP() AS updated_at
    ) AS source
    ON target.domain_id = source.domain_id
    WHEN NOT MATCHED THEN INSERT (
        domain_id, domain_name, description, criticality, owner_group, created_at, updated_at
    ) VALUES (
        source.domain_id, source.domain_name, source.description, source.criticality, source.owner_group, source.created_at, source.updated_at
    );

    -- 3. Ensure default party exists for ownership
    MERGE INTO GOV_PLATFORM.OWNERSHIP.DIM_PARTY AS target
    USING (
        SELECT 
            'DBT_SYSTEM' AS party_id,
            'DBT System' AS party_name,
            'SYSTEM' AS party_type,
            'dbt automated system processes' AS description,
            NULL AS contact_info,
            CURRENT_TIMESTAMP() AS created_at,
            CURRENT_TIMESTAMP() AS updated_at
    ) AS source
    ON target.party_id = source.party_id
    WHEN NOT MATCHED THEN INSERT (
        party_id, party_name, party_type, description, contact_info, created_at, updated_at
    ) VALUES (
        source.party_id, source.party_name, source.party_type, source.description, source.contact_info, source.created_at, source.updated_at
    );

    -- 4. Setup default classifications
    MERGE INTO GOV_PLATFORM.GOVERNANCE.CLASSIFICATION AS target
    USING (
        SELECT * FROM VALUES
            ('INTERNAL', 'Internal', 'Internal company data with standard access controls'),
            ('CONFIDENTIAL', 'Confidential', 'Confidential business data requiring restricted access'),
            ('PII', 'Personally Identifiable Information', 'Data containing personal identifiers requiring privacy protection'),
            ('PUBLIC', 'Public', 'Data approved for public consumption')
    ) AS source(class_id, class_name, description)
    ON target.class_id = source.class_id
    WHEN NOT MATCHED THEN INSERT (
        class_id, class_name, description, created_at, updated_at
    ) VALUES (
        source.class_id, source.class_name, source.description, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()
    );

    -- 5. Setup default DQ rules
    MERGE INTO GOV_PLATFORM.QUALITY.DQ_RULE AS target
    USING (
        SELECT 
            'row_count_check' AS rule_id,
            'Row Count Validation' AS rule_name,
            'COMPLETENESS' AS rule_type,
            NULL AS dataset_id,  -- Generic rule
            NULL AS column_name,
            PARSE_JSON('{"min_rows": 0, "description": "Validates table has data"}') AS config_json,
            'LOW' AS severity,
            'DBT_SYSTEM' AS owner_party_id,
            TRUE AS enabled_flag,
            CURRENT_TIMESTAMP() AS created_at,
            CURRENT_TIMESTAMP() AS updated_at
    ) AS source
    ON target.rule_id = source.rule_id
    WHEN NOT MATCHED THEN INSERT (
        rule_id, rule_name, rule_type, dataset_id, column_name, config_json, severity, owner_party_id, enabled_flag, created_at, updated_at
    ) VALUES (
        source.rule_id, source.rule_name, source.rule_type, source.dataset_id, source.column_name, source.config_json, source.severity, source.owner_party_id, source.enabled_flag, source.created_at, source.updated_at
    );

    {{ log("SF Gov Platform reference data setup completed", info=True) }}

  {%- endif -%}
{% endmacro %}
