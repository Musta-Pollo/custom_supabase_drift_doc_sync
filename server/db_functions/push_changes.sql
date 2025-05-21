CREATE OR REPLACE FUNCTION push_changes (
    _changes JSONB,
    last_pulled_at TIMESTAMP WITH TIME ZONE
) RETURNS TIMESTAMP WITH TIME ZONE AS $$
DECLARE
    _updated_collections TEXT[];
    _purgatory BOOLEAN;
    _user_id UUID;
    _now_utc TIMESTAMP WITH TIME ZONE;
    _sql_array TEXT[];
    _sql TEXT;
    _stmt TEXT;
BEGIN
    -- Log the input _changes
    RAISE LOG 'push_changes: Input _changes: %', _changes;

    -- GET USERID and current time
    SELECT auth.uid() INTO _user_id;
    _now_utc := NOW();

    -- Log the user ID and current time
    RAISE LOG 'push_changes: User ID: %, Current UTC time: %', _user_id, _now_utc;

    -- Look in all collections that include at least one created or updated element
    SELECT array_agg(key)
    INTO _updated_collections
    FROM jsonb_each(_changes)
    WHERE (
        -- Check if the 'created' array is not empty
        (jsonb_array_length(COALESCE(value->'created', '[]')) > 0) 
        -- Check if the 'updated' array is not empty
        OR (jsonb_array_length(COALESCE(value->'updated', '[]')) > 0)
    )
    AND to_regclass(format('%I.%I', split_part(key, '.', 1), split_part(key, '.', 2))) IS NOT NULL;

    -- Log updated collections after the filtering
    RAISE LOG 'push_changes: Updated Collections: %', _updated_collections;

    IF _updated_collections IS NOT NULL AND array_length(_updated_collections, 1) > 0 THEN
        -- Only perform the checks if last_pulled_at is NOT NULL
        IF last_pulled_at IS NOT NULL THEN
            -- Prevent updates or inserts out of sequence
            EXECUTE (
                'SELECT ' || string_agg(format($f$
                    EXISTS (
                        SELECT 1 FROM %I.%I
                        WHERE deleted_at IS NULL
                        AND server_updated_at > $2
                    )$f$, split_part(col, '.', 1), split_part(col, '.', 2)), ' OR ')
            )
            FROM UNNEST(_updated_collections) col
            USING _user_id, last_pulled_at INTO _purgatory;

            -- Log the purgatory check for updates
            RAISE LOG 'push_changes: Purgatory (updated records check): %', _purgatory;

            IF _purgatory THEN
                RAISE EXCEPTION 'push_changes: Record was updated remotely between pull and push';
            END IF;

            -- Prevent creation out of sequence
            EXECUTE (
                'SELECT ' || string_agg(format($f$
                    EXISTS (
                        SELECT 1 FROM %I.%I
                        WHERE deleted_at IS NULL
                        AND server_created_at > $2
                    )$f$, split_part(col, '.', 1), split_part(col, '.', 2)), ' OR ')
            )
            FROM UNNEST(_updated_collections) col
            USING _user_id, last_pulled_at INTO _purgatory;

            -- Log the purgatory check for created records
            RAISE LOG 'push_changes: Purgatory (created records check): %', _purgatory;

            IF _purgatory THEN
                RAISE EXCEPTION 'push_changes: Record was created remotely between pull and push';
            END IF;
        END IF;

        -- Get array of SQL statements
        _sql_array := construct_insert_update_sql(_updated_collections, _changes);

        -- Execute each SQL statement separately
        FOREACH _stmt IN ARRAY _sql_array
        LOOP
            -- Log the statement being executed
            RAISE LOG 'push_changes: Executing SQL: %', _stmt;
            
            -- Execute the statement
            EXECUTE _stmt USING _user_id, _now_utc, _changes;
        END LOOP;
    END IF;

        -- Handle deletes
    SELECT array_agg(key ORDER BY key)
    INTO _updated_collections
    FROM jsonb_each(_changes)
    WHERE COALESCE(value->'deleted', '[]') <> '[]'
    AND to_regclass(format('%I.%I', split_part(key, '.', 1), split_part(key, '.', 2))) IS NOT NULL;

    RAISE LOG 'push_changes: Collections to delete: %', _updated_collections;

    IF _updated_collections IS NOT NULL AND array_length(_updated_collections, 1) > 0 THEN
        -- Construct delete statements as an array
        _sql_array := (
            SELECT array_agg(
                format($f$
                    UPDATE %I.%I
                    SET deleted_at = $1
                    WHERE id::text = ANY(
                        SELECT jsonb_array_elements_text((($2)->%L)->'deleted')
                    )
                    AND deleted_at IS NULL
                $f$, split_part(collection, '.', 1), split_part(collection, '.', 2), collection)
            )
            FROM UNNEST(_updated_collections) collection
        );

        -- Execute each delete statement
        FOREACH _sql IN ARRAY _sql_array
        LOOP
            RAISE LOG 'push_changes: Executing delete SQL: %', _sql;
            EXECUTE _sql USING _now_utc, _changes;
        END LOOP;
    END IF;

    RETURN _now_utc;
END;
$$ LANGUAGE plpgsql;