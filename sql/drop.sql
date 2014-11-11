DROP TABLE IF EXISTS npi_licenses;
DROP TABLE IF EXISTS npi_locations;
DROP TABLE IF EXISTS npi_organization_officials;
DROP TABLE IF EXISTS npi_other_identifiers;
DROP TABLE IF EXISTS npi_parent_orgs;
DROP TABLE IF EXISTS npi_taxonomy_groups;
DROP TABLE IF EXISTS npis;
DROP TABLE IF EXISTS npi_files;
DROP TABLE IF EXISTS npi_indexed;

DELETE FROM data_sources WHERE source = 'NPI';