{{
    config(
        materialized='table'
    )
}}

-- If this is the first run, create a new comments table
-- Otherwise, this will be a no-op as it checks for the table's existence
{% if is_incremental() %}
    SELECT * FROM {{ this }}
{% else %}
    -- Initial creation of the comments table
    SELECT
        comment_id,
        entry_id,
        comment_text,
        created_by,
        created_at,
        updated_at
    FROM (
        SELECT 
            'init' as comment_id,
            'init' as entry_id,
            'Initial table creation' as comment_text,
            'system' as created_by,
            CURRENT_TIMESTAMP() as created_at,
            CURRENT_TIMESTAMP() as updated_at
    ) WHERE 1=0  -- Empty set for initial creation
{% endif %} 