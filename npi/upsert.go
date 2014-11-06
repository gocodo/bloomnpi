package npi

import (
	"database/sql"
	"fmt"
	"github.com/go-contrib/uuid"
	_ "github.com/lib/pq"
	"github.com/untoldone/bloomapi-npi/bloomdb"
	"github.com/untoldone/bloomapi-npi/csvHeaderReader"
	"io"
	"strconv"
	"strings"
	"sync"
	"time"
)

type tableDesc struct {
	name    string
	channel chan []string
	columns []string
}

var typeCodes = map[string]string {
	"1": "individual",
	"2": "organization",
}

var otherOrgCodes = map[string]string {
	"1": "individual",
  "2": "individual",
  "3": "organization",
  "4": "organization",
  "5": "individual+organization",
}

var lastNameCodes = map[string]string {
	"1": "former name",
  "2": "professional name",
  "3": "doing business as",
  "4": "former legal business name",
  "5": "other name",
}

var deactReasons = map[string]string {
	"DT": "death",
  "DB": "disbandment",
  "FR": "fraud",
  "OT": "other",
}

var genders = map[string]string {
	"M": "male",
	"F": "female",
}

var licenseNumberStateCodes = map[string]string {
  "S": "state",
  "T": "territory",
}

var taxonomySwitches = map[string]string {
  "X": "not answered",
  "Y": "yes",
  "N": "no",
}

var otherIdentifierCodes = map[string]string {
  "01": "other",
  "02": "medicare upin",
  "04": "medicare id-type unspecified",
  "05": "medicaid",
  "06": "medicare oscar/certification",
  "07": "medicare nsc",
  "08": "medicare pin",
}

var soleProprietorCodes = map[string]string {
  "X": "not answered",
  "Y": "yes",
  "N": "no",
}

var organizationSubpartCodes = map[string]string {
  "X": "not answered",
  "Y": "yes",
  "N": "no",
}

func makeKey(values ...string) string {
	key := "[" + strings.Join(values, "][") + "]"
	return uuid.NewV3(uuid.NamespaceOID, key).String()
}

func Upsert(file io.ReadCloser) {
	var wg sync.WaitGroup

	npis := make(chan []string, 100)
	npi_licenses := make(chan []string, 100)
	npi_locations := make(chan []string, 100)
	npi_organization_officials := make(chan []string, 100)
	npi_other_identifiers := make(chan []string, 100)
	npi_parent_orgs := make(chan []string, 100)
	npi_taxonomy_groups := make(chan []string, 100)

	wg.Add(1)
	go func() {
		defer wg.Done()
		reader := csvHeaderReader.NewReader(file)
		created_at := time.Now().Format(time.RFC3339)

		for {
			row, err := reader.Read()
			if err == io.EOF {
				break
			} else if err != nil {
				fmt.Println("Error: ", err)
				return
			}

			npi_id := makeKey(row.Value("NPI"))

			// Locations
			var business_location_id, practice_location_id string

			business_zip := row.Value("Provider Business Mailing Address Postal Code")
			if business_zip != "" {
				business_address := row.Value("Provider First Line Business Mailing Address")
				business_details := row.Value("Provider Second Line Business Mailing Address")
				business_city := row.Value("Provider Business Mailing Address City Name")
				business_state := row.Value("Provider Business Mailing Address State Name")
				business_country := row.Value("Provider Business Mailing Address Country Code (If outside U.S.)")
				business_phone := row.Value("Provider Business Mailing Address Telephone Number")
				business_fax := row.Value("Provider Business Mailing Address Fax Number")

				business_location_id = makeKey(business_address, business_details, business_city, business_country, business_phone, business_fax)

				business_location := make([]string, 9)
				business_location[0] = business_location_id
				business_location[1] = business_address
				business_location[2] = business_details
				business_location[3] = business_city
				business_location[4] = business_state
				business_location[5] = business_zip
				business_location[6] = business_country
				business_location[7] = business_phone
				business_location[8] = business_fax

				npi_locations <- business_location
			}

			practice_zip := row.Value("Provider Business Mailing Address Postal Code")
			if practice_zip != "" {
				practice_address := row.Value("Provider First Line Business Mailing Address")
				practice_details := row.Value("Provider Second Line Business Mailing Address")
				practice_city := row.Value("Provider Business Mailing Address City Name")
				practice_state := row.Value("Provider Business Mailing Address State Name")
				practice_country := row.Value("Provider Business Mailing Address Country Code (If outside U.S.)")
				practice_phone := row.Value("Provider Business Mailing Address Telephone Number")
				practice_fax := row.Value("Provider Business Mailing Address Fax Number")

				practice_location_id = makeKey(practice_address, practice_details, practice_city, practice_country, practice_phone, practice_fax)

				practice_location := make([]string, 9)
				practice_location[0] = practice_location_id
				practice_location[1] = practice_address
				practice_location[2] = practice_details
				practice_location[3] = practice_city
				practice_location[4] = practice_state
				practice_location[5] = practice_zip
				practice_location[6] = practice_country
				practice_location[7] = practice_phone
				practice_location[8] = practice_fax

				npi_locations <- practice_location
			}

			// Organization Officials
			var organization_official_id string

			official_last_name := row.Value("Authorized Official Last Name")
			if official_last_name != "" {
				first_name := row.Value("Authorized Official First Name")
				middle_name := row.Value("Authorized Official Middle Name")
				title := row.Value("Authorized Official Title or Position")
				telephone_number := row.Value("Authorized Official Telephone Number")
				name_prefix := row.Value("Authorized Official Name Prefix Text")
				name_suffix := row.Value("Authorized Official Name Suffix Text")
				credential := row.Value("Authorized Official Credential Text")

				organization_official_id = makeKey(official_last_name, first_name, middle_name, title, telephone_number, name_prefix, name_suffix, credential)

				organization_official := make([]string, 9)
				organization_official[0] = organization_official_id
				organization_official[1] = official_last_name
				organization_official[2] = first_name
				organization_official[3] = middle_name
				organization_official[4] = title
				organization_official[5] = telephone_number
				organization_official[6] = name_prefix
				organization_official[7] = name_suffix
				organization_official[8] = credential

				npi_organization_officials <- organization_official
			}

			// Other Identifiers
			for i := 1; i <= 50; i++ {
				identifier := row.Value("Other Provider Identifier_" + strconv.Itoa(i))
				if identifier != "" {
					var idType string
					var ok bool
					if idType, ok = otherIdentifierCodes[row.Value("Other Provider Identifier Type Code_" + strconv.Itoa(i))]; ok != true {
						idType = ""
					}

					state := row.Value("Other Provider Identifier State_" + strconv.Itoa(i))
					issuer := row.Value("Other Provider Identifier Issuer_" + strconv.Itoa(i))

					id := makeKey(npi_id, identifier, idType, state, issuer)

					other_identifier := make([]string, 6)
					other_identifier[0] = id
					other_identifier[1] = npi_id
					other_identifier[2] = identifier
					other_identifier[3] = idType
					other_identifier[4] = state
					other_identifier[5] = issuer

					npi_other_identifiers <- other_identifier
				}
			}

			// Licenses
			for i := 1; i <= 15; i++ {
				var ok bool
				var licenseState string
				if licenseState, ok = licenseNumberStateCodes[row.Value("Provider License Number State Code_" + strconv.Itoa(i))]; ok != true {
					licenseState = ""
				}

				var taxonomySwitch string
				if taxonomySwitch, ok = taxonomySwitches[row.Value("Healthcare Provider Primary Taxonomy Switch_" + strconv.Itoa(i))]; ok != true {
					taxonomySwitch = ""
				}

				taxonomy_code := row.Value("Healthcare Provider Taxonomy Code_" + strconv.Itoa(i))
				license_number := row.Value("Provider License Number_" + strconv.Itoa(i))

				if taxonomy_code != "" {
					npi_license_values := make([]string, 6)
					npi_license_values[0] = makeKey(npi_id, taxonomy_code, license_number, licenseState, taxonomySwitch)
					npi_license_values[1] = npi_id
					npi_license_values[2] = taxonomy_code
					npi_license_values[3] = license_number
					npi_license_values[4] = licenseState
					npi_license_values[5] = taxonomySwitch

					npi_licenses <- npi_license_values
				}
			}

			// Parent Organizations
			var parent_orgs_id string

			parent_business_name := row.Value("Parent Organization LBN")
			if parent_business_name != "" {
				tax_identification_number := row.Value("Parent Organization TIN")

				parent_orgs_id = makeKey(parent_business_name, tax_identification_number)

				parent_org := make([]string, 3)
				parent_org[0] = parent_orgs_id
				parent_org[1] = parent_business_name
				parent_org[2] = tax_identification_number

				npi_parent_orgs <- parent_org
			}

			// Taxonomy Groups
			for i := 1; i <= 15; i++ {
				taxonomy := row.Value("Healthcare Provider Taxonomy Group_" + strconv.Itoa(i))

				taxonomy_id := makeKey(taxonomy)

				taxonomy_group := make([]string, 3)
				taxonomy_group[0] = taxonomy_id
				taxonomy_group[1] = npi_id
				taxonomy_group[2] = taxonomy

				npi_taxonomy_groups <- taxonomy_group
			}

			var entity_type string
			var ok bool
			if entity_type, ok = typeCodes[row.Value("Entity Type Code")]; ok != true {
				entity_type = ""
			}

			var otherOrgType string
			if otherOrgType, ok = otherOrgCodes[row.Value("Provider Other Organization Name Type Code")]; ok != true {
				otherOrgType = ""
			}

			var lastNameType string
			if lastNameType, ok = lastNameCodes[row.Value("Provider Other Last Name Type Code")]; ok != true {
				lastNameType = ""
			}

			var deactReason string
			if deactReason, ok = deactReasons[row.Value("NPI Deactivation Reason Code")]; ok != true {
				deactReason = ""
			}

			var gender string
			if gender, ok = genders[row.Value("Provider Gender Code")]; ok != true {
				gender = ""
			}

			var soleProprietor string
			if soleProprietor, ok = soleProprietorCodes[row.Value("Is Sole Proprietor")]; ok != true {
				soleProprietor = ""
			}

			var subpart string
			if subpart, ok = organizationSubpartCodes[row.Value("Is Organization Subpart")]; ok != true {
				subpart = ""
			}

			npi_values := make([]string, 34)
			npi_values[0] = npi_id
			npi_values[1] = row.Value("NPI")
			npi_values[2] = created_at
			npi_values[3] = entity_type
			npi_values[4] = row.Value("Replacement NPI")
			npi_values[5] = row.Value("Employer Identification Number (EIN)")
			npi_values[6] = row.Value("Provider Organization Name (Legal Business Name)")
			npi_values[7] = row.Value("Provider Last Name (Legal Name)")
			npi_values[8] = row.Value("Provider First Name")
			npi_values[9] = row.Value("Provider Middle Name")
			npi_values[10] = row.Value("Provider Name Prefix Text")
			npi_values[11] = row.Value("Provider Name Suffix Text")
			npi_values[12] = row.Value("Provider Credential Text")
			npi_values[13] = row.Value("Provider Other Organization Name")
			npi_values[14] = otherOrgType
			npi_values[15] = row.Value("Provider Other Last Name")
			npi_values[16] = row.Value("Provider Other First Name")
			npi_values[17] = row.Value("Provider Other Middle Name")
			npi_values[18] = row.Value("Provider Other Name Prefix Text")
			npi_values[19] = row.Value("Provider Other Name Suffix Text")
			npi_values[20] = row.Value("Provider Other Credential Text")
			npi_values[21] = lastNameType
			npi_values[22] = row.Value("Provider Enumeration Date")
			npi_values[23] = row.Value("Last Update Date")
			npi_values[24] = deactReason
			npi_values[25] = row.Value("NPI Deactivation Date")
			npi_values[26] = row.Value("NPI Reactivation Date")
			npi_values[27] = gender
			npi_values[28] = soleProprietor
			npi_values[29] = subpart
			npi_values[30] = organization_official_id
			npi_values[31] = parent_orgs_id
			npi_values[32] = business_location_id
			npi_values[33] = practice_location_id

			npis <- npi_values
		}

		close(npis)
		close(npi_licenses)
		close(npi_locations)
		close(npi_organization_officials)
		close(npi_other_identifiers)
		close(npi_parent_orgs)
		close(npi_taxonomy_groups)
	}()

	dests := []tableDesc{
		tableDesc{
			name:    "npis",
			channel: npis,
			columns: []string{
				"id",
				"npi",
				"created_at",
				"type",
				"replacement_npi",
				"employer_identification_number",
				"business_name",
				"last_name",
				"first_name",
				"middle_name",
				"name_prefix",
				"name_suffix",
				"credential",
				"other_name",
				"other_name_type",
				"other_last_name",
				"other_first_name",
				"other_middle_name",
				"other_name_prefix",
				"other_name_suffix",
				"other_credential",
				"other_last_name_type",
				"enumeration_date",
				"last_update_date",
				"deactivation_reason",
				"deactivation_date",
				"reactivation_date",
				"gender",
				"sole_proprietor",
				"organization_subpart",
				"organization_official_id",
				"parent_orgs_id",
				"business_location_id",
				"practice_location_id",
			},
		},
		tableDesc{
			name:    "npi_licenses",
			channel: npi_licenses,
			columns: []string{
				"id",
				"npi_id",
				"healthcare_taxonomy_code",
				"license_number",
				"license_number_state",
				"taxonomy_switch",
			},
		},
		tableDesc{
			name:    "npi_locations",
			channel: npi_locations,
			columns: []string{
				"id",
				"address_line",
				"address_details_line",
				"city",
				"state",
				"zip",
				"country_code",
				"phone",
				"fax",
			},
		},
		tableDesc{
			name:    "npi_organization_officials",
			channel: npi_organization_officials,
			columns: []string{
				"id",
				"last_name",
				"first_name",
				"middle_name",
				"title",
				"telephone_number",
				"name_prefix",
				"name_suffix",
				"credential",
			},
		},
		tableDesc{
			name:    "npi_other_identifiers",
			channel: npi_other_identifiers,
			columns: []string{
				"id",
				"npi_id",
				"identifier",
				"type",
				"state",
				"issuer",
			},
		},
		tableDesc{
			name:    "npi_parent_orgs",
			channel: npi_parent_orgs,
			columns: []string{
				"id",
				"business_name",
				"tax_identification_number",
			},
		},
		tableDesc{
			name:    "npi_taxonomy_groups",
			channel: npi_taxonomy_groups,
			columns: []string{
				"id",
				"npi_id",
				"taxonomy",
			},
		},
	}

	for _, dest := range dests {
		wg.Add(1)
		go func(dest tableDesc) {
			defer wg.Done()
			db, err := sql.Open("postgres", "postgres://localhost/bloomapi-npi?sslmode=disable")
			if err != nil {
				fmt.Println("Error:", err)
				return
			}

			err = bloomdb.Upsert(db, dest.name, dest.columns, dest.channel)
			if err != nil {
				fmt.Println("Error:", err)
				return
			}
		}(dest)
	}

	wg.Wait()
}
