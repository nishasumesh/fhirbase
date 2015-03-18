-- #import ./perf_schema.sql
-- #import ../src/jsonbext.sql

func! random(a numeric, b numeric) RETURNS numeric
  SELECT ceil(a + (b - a) * random())::numeric;

func! random_elem(a anyarray) RETURNS anyelement
  SELECT a[1 + floor(RANDOM() * array_length(a, 1))];

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

func make_address(_street_name_ text, _zip_ text, _city_ text, _state_ text) RETURNS jsonb
  select array_to_json(ARRAY[
    json_build_object(
      'use', 'home',
      'line', ARRAY[_street_name_ || ' ' || this.random(0, 100)::text],
      'city', _city_,
      'postalCode', _zip_::text,
      'state', _state_,
      'country', 'US'
    )
  ])::jsonb;

func! insert_organizations() RETURNS bigint
  with organizations_source as (
    select organization_name, row_number() over ()
    from temp.organization_names
    order by random()
  ), street_names_source as (
    select street_name, row_number() over ()
    from temp.street_names
    order by random()
  ), cities_source as (
    select city, zip, state, row_number() over ()
    from temp.cities
    order by random()
  ), organization_data as (
    select *,
           this.random_phone() as phone
    from organizations_source
    join street_names_source using (row_number)
    join cities_source using (row_number)
  ), inserted as (
    INSERT into organization (logical_id, version_id, content)
    SELECT obj->>'id', obj#>>'{meta,versionId}', obj
    FROM (
      SELECT
        json_build_object(
         'resourceType', 'Organization',
         'id', gen_random_uuid(),
         'name', organization_name,
         'telecom', ARRAY[
           json_build_object(
            'system', 'phone',
            'value', phone,
            'use', 'work'
           )
         ],
         'address', this.make_address(street_name, zip, city, state)
        )::jsonb as obj
        FROM organization_data
    ) _
    RETURNING logical_id
  )
  select count(*) inserted;

func! insert_practitioner(_total_count_ integer) RETURNS bigint
  with first_names_source as (
    select *, row_number() over () from (
      select CASE WHEN sex = 'M' THEN 'male' ELSE 'female' END as sex,
             first_name
      from temp.first_names
      order by random()
      limit _total_count_) _
  ), last_names_source as (
    select *, row_number() over () from (
      select last_name
      from temp.last_names
      order by random()
      limit _total_count_) _
  ), practitioner_data as (
    select *
    from first_names_source
    join last_names_source using (row_number)
  ), inserted as (
    INSERT into practitioner (logical_id, version_id, content)
    SELECT obj->>'id', obj#>>'{meta,versionId}', obj
    FROM (
      SELECT
        json_build_object(
         'resourceType', 'Practitioner',
         'id', gen_random_uuid(),
         'name', ARRAY[
           json_build_object(
            'given', ARRAY[first_name],
            'family', ARRAY[last_name]
           )
         ]
        )::jsonb as obj
        FROM practitioner_data
    ) _
    RETURNING logical_id
  )
  select count(*) from practitioner_data;

func! insert_patients(_total_count_ integer) RETURNS bigint
  with first_names_source as (
    select CASE WHEN sex = 'M' THEN 'male' ELSE 'female' END as sex,
           first_name,
           row_number() over ()
    from temp.first_names
    cross join generate_series(0, ceil(_total_count_::float
                                       / (select count(*)
                                          from temp.first_names)::float)::integer)
    order by random()
  ), last_names_source as (
    select last_name, row_number() over ()
    from temp.last_names
    cross join generate_series(0, ceil(_total_count_::float
                                       / (select count(*)
                                          from temp.last_names)::float)::integer)
    order by random()
  ), street_names_source as (
    select street_name, row_number() over ()
    from temp.street_names
    cross join generate_series(0, ceil(_total_count_::float
                                       / (select count(*)
                                          from temp.street_names)::float)::integer)
    order by random()
  ), cities_source as (
    select city, zip, state, row_number() over ()
    from temp.cities
    cross join generate_series(0, ceil(_total_count_::float
                                       / (select count(*)
                                          from temp.cities)::float)::integer)
    order by random()
  ), languages_source as (
    select code as language_code,
           name as language_name,
           row_number() over ()
    from temp.languages
    cross join generate_series(0, ceil(_total_count_::float
                                       / (select count(*)
                                          from temp.languages)::float)::integer)
    order by random()
  ), organizations_source as (
    select logical_id as organization_id,
           content#>>'{name}' as organization_name,
           row_number() over ()
    from organization
    cross join generate_series(0, ceil(_total_count_::float
                                       / (select count(*)
                                          from organization)::float)::integer)
    order by random()
  ), patient_data as (
    select
      *,
      this.random_date() as birth_date,
      this.random_phone() as phone
    from first_names_source
    join last_names_source using (row_number)
    join street_names_source using (row_number)
    join cities_source using (row_number)
    join languages_source using (row_number)
    join organizations_source using (row_number)
  ), inserted as (
    INSERT into patient (logical_id, version_id, content)
    SELECT obj->>'id', obj#>>'{meta,versionId}', obj
    FROM (
      SELECT
        json_build_object(
         'resourceType', 'Patient',
         'id', gen_random_uuid(),
         'meta', json_build_object(
            'versionId', gen_random_uuid(),
            'lastUpdated', CURRENT_TIMESTAMP
          ),
         'gender', sex,
         'birthDate', birth_date,
         'active', TRUE,
         'name', ARRAY[
           json_build_object(
            'given', ARRAY[first_name],
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
         'address', this.make_address(street_name, zip, city, state),
         'communication', ARRAY[
           json_build_object(
             'language',
             json_build_object(
               'coding', ARRAY[
                 json_build_object(
                   'system', 'urn:ietf:bcp:47',
                   'code', language_code,
                   'display', language_name
                 )
               ],
               'text', language_name
             ),
             'preferred', TRUE
           )
         ],
         'identifier', ARRAY[
           json_build_object(
             'use', 'usual',
             'system', 'urn:oid:2.16.840.1.113883.2.4.6.3',
             'value', this.random(6000000, 100000000)::text
           ),
           json_build_object(
             'use', 'usual',
             'system', 'urn:oid:1.2.36.146.595.217.0.1',
             'value', this.random(6000000, 100000000)::text,
             'label', 'MRN'
           )
         ],
         'managingOrganization', json_build_object(
           'reference', 'Organization/' || organization_id,
           'display', organization_name
         )
        )::jsonb as obj
        FROM patient_data
        LIMIT _total_count_
    ) _
    RETURNING logical_id
  )
  select count(*) inserted;

func! insert_encounters() RETURNS bigint
  with patients_ids_source as (
    (select logical_id as patient_id,
           row_number() over ()
     from patient)

    UNION ALL

    (select logical_id as patient_id,
            row_number() over ()
     from patient
    order by random()
    limit (select count(*) from patient) / 3)
  ), practitioners_source as (
    select logical_id as practitioner_id,
           row_number() over ()
    from practitioner
    order by random()
  ), encounter_data as (
    select *,
           this.random_elem(ARRAY['inpatient',
                                  'outpatient',
                                  'ambulatory',
                                  'emergency']) as class,
           this.random_elem(ARRAY['in-progress',
                                  'planned',
                                  'arrived',
                                  'onleave',
                                  'cancelled',
                                  'finished']) as status
    from patients_ids_source
    join practitioners_source using (row_number)
  ), inserted as (
    INSERT into encounter (logical_id, version_id, content)
    SELECT obj->>'id', obj#>>'{meta,versionId}', obj
    FROM (
      SELECT
        json_build_object(
         'resourceType', 'Encounter',
         'id', gen_random_uuid(),
         'status', status,
         'class', class,
         'patient', json_build_object(
           'reference', 'Patient/' || patient_id
         ),
         'participant', ARRAY[
           json_build_object(
             'individual', json_build_object(
               'reference', 'Practitioner/' || practitioner_id
             )
           )
         ]
        )::jsonb as obj
        FROM encounter_data
    ) _
    RETURNING logical_id
  )
  select count(*) inserted;

DO language plpgsql $$
BEGIN
  RAISE NOTICE 'Create Patient';
END
$$;

SELECT count(crud.create('{}'::jsonb, jsonbext.dissoc(patients.content, 'id'))) FROM
(SELECT content FROM patient LIMIT 1000) patients;

DO language plpgsql $$
BEGIN
  RAISE NOTICE 'Read Patient';
END
$$;

SELECT count(crud.read('{}'::jsonb, patients.logical_id)) FROM
(SELECT logical_id FROM patient LIMIT 1) patients;

DO language plpgsql $$
BEGIN
  RAISE NOTICE 'Update Patient';
END
$$;

SELECT crud.update('{}'::jsonb, temp_patients.data)
FROM (SELECT data FROM temp.patient_data limit 1) temp_patients;

-- DO language plpgsql $$
-- BEGIN
--   RAISE NOTICE 'Update Patient';
-- END
-- $$;

-- drop table if exists temp.patient_data;
-- create table temp.patient_data (data jsonb);
-- insert into temp.patient_data (data)
-- select jsonbext.merge(content,
--                       '{"multipleBirthBoolean": true}'::jsonb)
-- from patient limit 1000;

-- select crud.update('{}'::jsonb, temp_patients.data)
-- from (select data from temp.patient_data) temp_patients;

-- SELECT count(crud.update('{}'::jsonb, jsonbext.assoc('{"resourceType": "Patient", "text": {"status": "generated", "div": "<div>!-- Snipped for Brevity --></div>"}, "extension": [{"url": "http://hl7.org/fhir/StructureDefinition/patient-birthTime", "valueInstant": "2001-05-06T14:35:45-05:00"}], "identifier": [{"use": "usual", "label": "MRN", "system": "urn:oid:1.2.36.146.595.217.0.1", "value": "12345", "period": {"start": "2001-05-06"}, "assigner": {"display": "Acme Healthcare"}}], "name": [{"use": "official", "family": ["Chalmers"], "given": ["Peter", "James"]}, {"use": "usual", "given": ["Jim"]}], "telecom": [{"use": "home"}, {"system": "phone", "value": "(03) 5555 6473", "use": "work"}], "gender": "male", "birthDate": "1974-12-25", "deceasedBoolean": false, "address": [{"use": "home", "line": ["534 Erewhon St"], "city": "PleasantVille", "state": "Vic", "postalCode": "3999"}], "contact": [{"relationship": [{"coding": [{"system": "http://hl7.org/fhir/patient-contact-relationship", "code": "partner"}]}], "name": {"family": ["du", "Marché"], "_family": [{"extension": [{"url": "http://hl7.org/fhir/StructureDefinition/iso21090-EN-qualifier", "valueCode": "VV"}]}, null], "given": ["Bénédicte"]}, "telecom": [{"system": "phone", "value": "+33 (237) 998327"}]}], "active": true}'::jsonb, 'id'::text, patients.content#>'{id}'))) FROM
-- (SELECT content FROM patient LIMIT 1000) patients;

-- SELECT fhir.update(...cfg..., ...);

-- SELECT fhir.delete(...cfg..., 'Patient', ...id...);

-- SELECT fhir.search('Patient', 'name=John');

-- SELECT indexing.index_search_param('Patient','name');
-- SELECT fhir.search('Patient', 'name=John');

-- select admin.admin_disk_usage_top(10);
