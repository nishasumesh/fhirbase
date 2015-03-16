-- #import ../src/fhir.sql
create schema if not exists temp;

drop table if exists temp.first_names;
drop table if exists temp.last_names;
drop table if exists temp.languages;
drop table if exists temp.street_names;

create table temp.first_names (
  sex text,
  first_name text
);

create table temp.last_names (
  last_name text
);

create table temp.languages (
  code text,
  name text
);

create table temp.street_names (
  street_name text
);

\copy temp.first_names (sex, first_name) from './perf/data/first_names_shuffled.csv';
\copy temp.last_names (last_name) from './perf/data/last_names_shuffled.csv';
\copy temp.languages (code, name) from './perf/data/language-codes-iso-639-1-alpha-2.csv' with csv;
\copy temp.street_names (street_name) from './perf/data/street_names.csv';

select count(*) from temp.first_names;
select count(*) from temp.last_names;
select count(*) from temp.languages;
select count(*) from temp.street_names;

create table if not exists temp.patient_names (
  sex text,
  first_name text,
  last_name text,
  language_code text,
  language_name text,
  street_name text
);

\set patients_total_count `echo $patients_total_count`

with first_name_source as (
  select sex, first_name
  from temp.first_names
  -- limit ceil(pow((:'patients_total_count')::float, 1.0/4.0))
), last_name_source as (
  select last_name
  from temp.last_names
  -- limit ceil(pow((:'patients_total_count')::float, 1.0/4.0))
), languages_source as (
  select code, name
  from temp.languages
  -- limit ceil(pow((:'patients_total_count')::float, 1.0/4.0))
), street_names_source as (
  select street_name
  from temp.street_names
  -- limit ceil(pow((:'patients_total_count')::float, 1.0/4.0))
)
INSERT into temp.patient_names (sex, first_name, last_name, language_code, language_name, street_name)
SELECT * FROM (
  select sex, first_name, last_name, language_code, language_name, street_name from (
    select _first_name.first_name, _last_name.last_name,
           CASE WHEN _first_name.sex = 'M' THEN 'male' ELSE 'female' END as sex,
           _language.code as language_code, _language.name as language_name,
           _street_name.street_name
    from first_name_source as _first_name
    cross join last_name_source as _last_name
    cross join street_names_source as _street_name
    cross join languages_source as _language) _
  where not exists (select * from temp.patient_names)
) __
ORDER BY RANDOM();

select fhir.generate_tables('{Patient}');
