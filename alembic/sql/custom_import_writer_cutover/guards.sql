-- Native historical FK/PK/UNIQUE/NOT NULL/CHECK constraints are untouched.
-- Every statement TRUNCATE guard and every low-volume control guard survives.
DO $cutover$
DECLARE spec record; trigger_row record;
BEGIN
    FOR spec IN SELECT * FROM jsonb_to_recordset(__GUARDS__)
        AS specs("table" text,"trigger" text,"function" text,events integer)
    LOOP
        SELECT t.*,p.proname,p.pronamespace INTO trigger_row
        FROM pg_trigger t JOIN pg_proc p ON p.oid=t.tgfoid
        WHERE t.tgrelid=to_regclass(format('%I.%I',__CONTROL_LITERAL__,spec."table"))
            AND t.tgname=spec."trigger";
        IF NOT FOUND OR trigger_row.tgisinternal OR trigger_row.tgtype<>spec.events
            OR trigger_row.proname<>spec."function"
            OR trigger_row.pronamespace<>to_regnamespace(__CONTROL_LITERAL__) THEN
            RAISE EXCEPTION 'custom_import_cutover_guard_inventory_mismatch: %.%',spec."table",spec."trigger"; END IF;
    END LOOP;
    IF EXISTS(SELECT 1 FROM pg_trigger t JOIN pg_class c ON c.oid=t.tgrelid
        JOIN pg_namespace n ON n.oid=c.relnamespace
        WHERE n.nspname=__CONTROL_LITERAL__ AND c.relname=ANY(__HOT_TABLES__)
            AND NOT t.tgisinternal AND (t.tgtype&1)=1
            AND NOT EXISTS(SELECT 1 FROM jsonb_to_recordset(__GUARDS__)
                AS specs("table" text,"trigger" text,"function" text,events integer)
                WHERE specs."table"=c.relname AND specs."trigger"=t.tgname)) THEN
        RAISE EXCEPTION 'custom_import_cutover_unknown_record_guard'; END IF;
    IF EXISTS(SELECT 1 FROM unnest(__HOT_TABLES__) tables(name)
        WHERE NOT EXISTS(SELECT 1 FROM pg_trigger t
            WHERE t.tgrelid=to_regclass(format('%I.%I',__CONTROL_LITERAL__,tables.name))
                AND NOT t.tgisinternal AND (t.tgtype&1)=0 AND (t.tgtype&32)=32 AND t.tgenabled='A')) THEN
        RAISE EXCEPTION 'custom_import_cutover_truncate_guard_missing'; END IF;
    FOR spec IN SELECT * FROM jsonb_to_recordset(__GUARDS__)
        AS specs("table" text,"trigger" text,"function" text,events integer)
    LOOP
        EXECUTE format('DROP TRIGGER %I ON %I.%I',spec."trigger",__CONTROL_LITERAL__,spec."table");
    END LOOP;
    IF EXISTS(SELECT 1 FROM pg_trigger t JOIN pg_class c ON c.oid=t.tgrelid
        JOIN pg_namespace n ON n.oid=c.relnamespace
        WHERE n.nspname=__CONTROL_LITERAL__ AND c.relname=ANY(__HOT_TABLES__)
            AND NOT t.tgisinternal AND (t.tgtype&1)=1) THEN
        RAISE EXCEPTION 'custom_import_cutover_record_guard_survived'; END IF;
END $cutover$;
