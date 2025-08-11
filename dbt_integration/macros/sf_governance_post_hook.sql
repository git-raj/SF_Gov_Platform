{% macro sf_governance_post_hook() %}
  {%- if execute -%}
    -- SF Gov Platform DBT Post-Hook Macro
    -- Syncs dbt model metadata to GOV_PLATFORM governance database
    
    {% set current_timestamp = modules.datetime.datetime.now().isoformat() %}
    {% set model_database = this.database %}
    {% set model_schema = this.schema %}
    {% set model_table = this.identifier %}
    {% set full_model_name = model_database ~ '.' ~ model_schema ~ '.' ~ model_table %}
    
    -- 1. Insert/Update Dataset in CATALOG.DIM_DATASET
    MERGE INTO GOV_PLATFORM.CATALOG.DIM_DATASET AS target
    USING (
        SELECT 
            'dbt_' || '{{ model_database }}' || '_' || '{{ model_schema }}' || '_' || '{{ model_table }}' AS dataset_id,
            '{{ full_model_name }}' AS catalog_name,
            'DBT_{{ target.name | upper }}' AS system_id,
            'ANALYTICS' AS domain_id,  -- Default domain, can be overridden by model config
            'SNOWFLAKE' AS platform,
            '{{ model_database }}' AS database_name,
            '{{ model_schema }}' AS schema_name,
            '{{ model_table }}' AS object_name,
            '{{ config.materialized | upper }}' AS object_type,
            CASE 
                WHEN '{{ model_schema }}' LIKE '%raw%' OR '{{ model_schema }}' LIKE '%bronze%' THEN 'BRONZE'
                WHEN '{{ model_schema }}' LIKE '%staging%' OR '{{ model_schema }}' LIKE '%silver%' THEN 'SILVER'
                WHEN '{{ model_schema }}' LIKE '%marts%' OR '{{ model_schema }}' LIKE '%gold%' THEN 'GOLD'
                ELSE 'SILVER'
            END AS dv_layer,
            CASE 
                WHEN '{{ model.config.get("meta", {}).get("classification", "") }}' != '' 
                THEN '{{ model.config.get("meta", {}).get("classification", "") }}'
                ELSE 'INTERNAL'
            END AS classification,
            {% if model.config.get("meta", {}).get("contains_pii", false) %}TRUE{% else %}FALSE{% endif %} AS is_cde,
            '{{ model.description | replace("'", "''") if model.description else "dbt model " ~ model_table }}' AS description,
            NULL AS retention_policy_id,
            'DRAFT' AS certification,
            CURRENT_TIMESTAMP() AS created_at,
            CURRENT_TIMESTAMP() AS updated_at
    ) AS source
    ON target.dataset_id = source.dataset_id
    WHEN MATCHED THEN UPDATE SET
        catalog_name = source.catalog_name,
        object_type = source.object_type,
        dv_layer = source.dv_layer,
        classification = source.classification,
        is_cde = source.is_cde,
        description = source.description,
        updated_at = source.updated_at
    WHEN NOT MATCHED THEN INSERT (
        dataset_id, catalog_name, system_id, domain_id, platform,
        database_name, schema_name, object_name, object_type, dv_layer,
        classification, is_cde, description, retention_policy_id, 
        certification, created_at, updated_at
    ) VALUES (
        source.dataset_id, source.catalog_name, source.system_id, source.domain_id, source.platform,
        source.database_name, source.schema_name, source.object_name, source.object_type, source.dv_layer,
        source.classification, source.is_cde, source.description, source.retention_policy_id,
        source.certification, source.created_at, source.updated_at
    );

    -- 2. Create Lineage Node for this model
    {% set dataset_id = 'dbt_' ~ model_database ~ '_' ~ model_schema ~ '_' ~ model_table %}
    MERGE INTO GOV_PLATFORM.LINEAGE.LINEAGE_NODE AS target
    USING (
        SELECT 
            '{{ dataset_id }}' AS node_id,
            'DATASET' AS node_type,
            '{{ dataset_id }}' AS ref_id,
            '{{ full_model_name }}' AS name,
            '{{ model.description | replace("'", "''") if model.description else "dbt model " ~ model_table }}' AS description,
            CURRENT_TIMESTAMP() AS created_at,
            CURRENT_TIMESTAMP() AS updated_at
    ) AS source
    ON target.node_id = source.node_id
    WHEN MATCHED THEN UPDATE SET
        name = source.name,
        description = source.description,
        updated_at = source.updated_at
    WHEN NOT MATCHED THEN INSERT (
        node_id, node_type, ref_id, name, description, created_at, updated_at
    ) VALUES (
        source.node_id, source.node_type, source.ref_id, source.name, source.description, source.created_at, source.updated_at
    );

    -- 3. Create Lineage Edges for dependencies
    {% if model.depends_on.nodes %}
        {% for dependency in model.depends_on.nodes %}
            {% set dep_parts = dependency.split('.') %}
            {% if dep_parts | length >= 2 %}
                {% set dep_name = dep_parts[-1] %}
                {% set source_node_id = 'dbt_' ~ model_database ~ '_' ~ model_schema ~ '_' ~ dep_name %}
                
                MERGE INTO GOV_PLATFORM.LINEAGE.LINEAGE_EDGE AS target
                USING (
                    SELECT 
                        'edge_' || '{{ source_node_id }}' || '_to_' || '{{ dataset_id }}' AS edge_id,
                        '{{ source_node_id }}' AS src_node_id,
                        '{{ dataset_id }}' AS tgt_node_id,
                        'TRANSFORMS' AS edge_type,
                        'dbt_model_{{ model_table }}' AS logic_ref,
                        TRUE AS active_flag,
                        CURRENT_TIMESTAMP() AS created_at,
                        CURRENT_TIMESTAMP() AS updated_at
                ) AS source
                ON target.edge_id = source.edge_id
                WHEN MATCHED THEN UPDATE SET
                    logic_ref = source.logic_ref,
                    active_flag = source.active_flag,
                    updated_at = source.updated_at
                WHEN NOT MATCHED THEN INSERT (
                    edge_id, src_node_id, tgt_node_id, edge_type, logic_ref, active_flag, created_at, updated_at
                ) VALUES (
                    source.edge_id, source.src_node_id, source.tgt_node_id, source.edge_type, source.logic_ref, source.active_flag, source.created_at, source.updated_at
                );
            {% endif %}
        {% endfor %}
    {% endif %}

    -- 4. Record Column-Level Metadata in MAP_DATASET_ATTRIBUTE
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
        AND table_name = '{{ model_table }}'
        ORDER BY ordinal_position
    {% endset %}

    {% if execute %}
        {% set results = run_query(columns_query) %}
        {% if results %}
            {% for row in results.rows %}
                MERGE INTO GOV_PLATFORM.CATALOG.MAP_DATASET_ATTRIBUTE AS target
                USING (
                    SELECT 
                        'attr_' || '{{ dataset_id }}' || '_' || '{{ row[0] }}' AS map_id,
                        '{{ dataset_id }}' AS dataset_id,
                        '{{ row[0] }}' AS column_name,
                        NULL AS term_id,  -- To be mapped separately
                        NULL AS cde_id,   -- To be mapped separately
                        CASE 
                            WHEN UPPER('{{ row[0] }}') LIKE '%EMAIL%' THEN 'EMAIL'
                            WHEN UPPER('{{ row[0] }}') LIKE '%PHONE%' THEN 'PHONE'
                            WHEN UPPER('{{ row[0] }}') LIKE '%SSN%' OR UPPER('{{ row[0] }}') LIKE '%SOCIAL%' THEN 'SSN'
                            WHEN UPPER('{{ row[0] }}') LIKE '%ID' THEN 'IDENTIFIER'
                            WHEN '{{ row[1] }}' LIKE '%TIMESTAMP%' OR '{{ row[1] }}' LIKE '%DATE%' THEN 'TEMPORAL'
                            ELSE 'GENERAL'
                        END AS semantic_type,
                        {% if model.config.get("meta", {}).get("quality_critical_columns", []) %}
                            CASE WHEN '{{ row[0] }}' IN ({{ model.config.get("meta", {}).get("quality_critical_columns", []) | map("string") | join(", ") }}) THEN TRUE ELSE FALSE END
                        {% else %}
                            FALSE
                        {% endif %} AS quality_critical,
                        COALESCE('{{ row[5] if row[5] else "" }}', '') AS description,
                        CURRENT_TIMESTAMP() AS created_at,
                        CURRENT_TIMESTAMP() AS updated_at
                ) AS source
                ON target.map_id = source.map_id
                WHEN MATCHED THEN UPDATE SET
                    semantic_type = source.semantic_type,
                    quality_critical = source.quality_critical,
                    description = source.description,
                    updated_at = source.updated_at
                WHEN NOT MATCHED THEN INSERT (
                    map_id, dataset_id, column_name, term_id, cde_id, semantic_type, quality_critical, description, created_at, updated_at
                ) VALUES (
                    source.map_id, source.dataset_id, source.column_name, source.term_id, source.cde_id, source.semantic_type, source.quality_critical, source.description, source.created_at, source.updated_at
                );
            {% endfor %}
        {% endif %}
    {% endif %}

    -- 5. Record Quality Metrics (Row Count)
    {% set row_count_query %}
        SELECT COUNT(*) as row_count FROM {{ this }}
    {% endset %}
    
    {% set row_count_result = run_query(row_count_query) %}
    {% if row_count_result %}
        INSERT INTO GOV_PLATFORM.QUALITY.DQ_RESULT (
            run_id,
            rule_id,
            dataset_id,
            outcome,
            metrics_summary,
            evidence_ref,
            created_at,
            updated_at
        )
        SELECT 
            'dbt_run_' || CURRENT_TIMESTAMP()::VARCHAR || '_' || '{{ dataset_id }}' AS run_id,
            'row_count_check' AS rule_id,
            '{{ dataset_id }}' AS dataset_id,
            CASE WHEN {{ row_count_result.rows[0][0] }} >= 0 THEN 'PASS' ELSE 'FAIL' END AS outcome,
            PARSE_JSON('{"row_count": {{ row_count_result.rows[0][0] }}, "check_type": "volume", "model": "{{ model_table }}"}') AS metrics_summary,
            '{{ full_model_name }}' AS evidence_ref,
            CURRENT_TIMESTAMP() AS created_at,
            CURRENT_TIMESTAMP() AS updated_at
        WHERE NOT EXISTS (
            SELECT 1 FROM GOV_PLATFORM.QUALITY.DQ_RESULT 
            WHERE dataset_id = '{{ dataset_id }}' 
            AND rule_id = 'row_count_check'
            AND DATE(created_at) = CURRENT_DATE()
        );
    {% endif %}

    -- Log successful processing
    {{ log("SF Gov Platform metadata updated for model: " ~ full_model_name, info=True) }}

  {%- endif -%}
{% endmacro %}
