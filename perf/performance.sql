-- #import ./perf_schema.sql

func! random(a numeric, b numeric) RETURNS numeric
  SELECT ceil(a + (b - a) * random())::numeric;

func random_elem(a anyarray) RETURNS anyelement
  SELECT a[floor(RANDOM() * array_length(a, 1))];

func! random_date() RETURNS text
  SELECT this.random(1900, 2010)::text
           || '-'
           || lpad(this.random(1, 12)::text, 2, '0')
           || '-'
           || lpad(this.random(1, 28)::text, 2, '0');

func! random_phone() RETURNS text
  SELECT '+' || this.random(1, 12)::text ||
         ' (' || this.random(1, 999)::text || ') ' ||
         lpad(this.random(1, 999)::text, 3, '0') ||
         '-' ||
         lpad(this.random(1, 99)::text, 2, '0') ||
         '-' ||
         lpad(this.random(1, 99)::text, 2, '0')

-- TODO: improve generator
--       improve patient resource (add adress etc.)
--       add more resources (encounter, order etc.)
func! insert_patients(_total_count_ integer, _offset_ integer) RETURNS bigint
  WITH
  -- x as (
  --   SELECT * from temp.patient_names
  --    OFFSET _offset_
  --    LIMIT _total_count_
  -- ),
  patient_data as (
    select
           -- x.first_name as given_name,
           -- x.last_name as family_name,
           -- x.sex as gender,
           this.random_date() as birth_date,
           this.random_phone() as phone,
           this.random_elem(languages) as language,
           this.random_elem(street_names) as street_name,
           this.random_elem(first_names) as first_name,
           this.random_elem(last_names) as last_name
    from generate_series(0, _total_count_), (
      SELECT array_agg(languages) as languages FROM temp.languages
    ) __, (
      SELECT array_agg(street_name) as street_names FROM temp.street_names
    ) ___, (
      SELECT array_agg(first_names) as first_names FROM temp.first_names
    ) ____, (
      SELECT array_agg(last_name) as last_names FROM temp.last_names
    ) _____
  ), inserted as (
    INSERT into patient (logical_id, version_id, content)
    SELECT obj->>'id', obj#>>'{meta,versionId}', obj
    FROM (
      SELECT
        json_build_object(
         'id', gen_random_uuid(),
         'meta', json_build_object(
            'versionId', gen_random_uuid(),
            'lastUpdated', CURRENT_TIMESTAMP
          ),
         'resourceType', 'Patient',
         'gender', (first_name).sex,
         'birthDate', birth_date,
         'name', ARRAY[
           json_build_object(
            'given', ARRAY[(first_name).first_name],
            'family', ARRAY[last_name]
           )
         ],
         'telecom', ARRAY[
           json_build_object(
            'system', 'phone',
            'value', phone,
            'use', 'home'
           )
         ],
         'address', ARRAY[
           json_build_object(
             'use', 'home',
             'line', ARRAY[street_name || ' ' || this.random(0, 100)::text],
             'city', 'Amsterdam',
             'postalCode', '1024 RJ',
             'country', 'NLD'
           )
         ],
         'communication', ARRAY[
           json_build_object(
             'language',
             json_build_object(
               'coding', ARRAY[
                 json_build_object(
                   'system', 'urn:ietf:bcp:47',
                   'code', (language).code,
                   'display', (language).name
                 )
               ],
               'text', (language).name
             ),
             'preferred', TRUE
           )
         ]
        )::jsonb as obj
        FROM patient_data
        LIMIT _total_count_
    ) _
    RETURNING logical_id
  )
  select count(*) inserted;

\timing
\set batch_size `echo $batch_size`
\set batch_number `echo $batch_number`
\set rand_seed `echo ${rand_seed:-0.321}`

SELECT setseed(:'rand_seed'::float);

-- select this.insert_patients((:'batch_size')::int,
--                              (:'batch_number')::int);
-- select count(*) from patient;

-- SELECT fhir.search('Patient', 'name=John');

-- SELECT indexing.index_search_param('Patient','name');
-- SELECT fhir.search('Patient', 'name=John');

-- select admin.admin_disk_usage_top(10);
