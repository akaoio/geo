/*
1. Transform contryInfo.txt to data/countries.json which is an array of countries' geonameid
2. For each country:
    - create a json file named under its geonameid in data/{geonameid}.json and this file has all the info of the country, and its direct chidren in geonameids array
    - create a json file named under its country code in data/{countrycode}.json and this file only has the name and geonameid of the country like this { "id": geonameid, "name": country name }
    - unzip its corresponding zip file from raw/ and for each record in the unzipped data file, create a new json file data/{geonameid}.json that has this schema: { "id": its geonameid, "parent": its parent geonameid, "children": [array of its direct children geonameids], "name": name, "level": its adm level (0 for country, 1 for admin1, 2 for admin2, etc) }
By doing so, we are able to load country -> deeper levels, and also from a deeper level, we can go up to its country easily.
country json files and other location json files have same schema
*/

import { readFile, writeFile, mkdir } from 'fs/promises';
import { dirname, join } from 'path';
import { fileURLToPath } from 'url';
import { existsSync } from 'fs';
import AdmZip from 'adm-zip';

const __dirname = dirname(fileURLToPath(import.meta.url));
const RAW_DIR = join(__dirname, 'raw');
const DATA_DIR = join(__dirname, 'data');

// Feature code to level mapping
const FEATURE_LEVELS = {
  'PCLI': 0,  // independent political entity (country)
  'ADM1': 1,  // first-order administrative division
  'ADM2': 2,  // second-order administrative division
  'ADM3': 3,  // third-order administrative division
  'ADM4': 4,  // fourth-order administrative division
  'ADM5': 5,  // fifth-order administrative division
};

/**
 * Parse countryInfo.txt and extract country data
 */
async function parseCountryInfo() {
  console.log('Parsing countryInfo.txt...');
  
  const countryInfoPath = join(RAW_DIR, 'countryInfo.txt');
  const content = await readFile(countryInfoPath, 'utf-8');
  const lines = content.split('\n');
  
  const countries = [];
  
  for (const line of lines) {
    // Skip comments and empty lines
    if (line.startsWith('#') || line.trim() === '') {
      continue;
    }
    
    // Tab-delimited format
    const fields = line.split('\t');
    
    if (fields.length < 18) {
      continue; // Skip malformed lines
    }
    
    const country = {
      iso: fields[0],
      iso3: fields[1],
      isoNumeric: fields[2],
      fips: fields[3],
      name: fields[4],
      capital: fields[5],
      area: fields[6],
      population: fields[7],
      continent: fields[8],
      tld: fields[9],
      currencyCode: fields[10],
      currencyName: fields[11],
      phone: fields[12],
      postalCodeFormat: fields[13],
      postalCodeRegex: fields[14],
      languages: fields[15],
      geonameid: fields[16],
      neighbours: fields[17],
      equivalentFipsCode: fields[18] || '',
    };
    
    countries.push(country);
  }
  
  console.log(`✓ Parsed ${countries.length} countries`);
  return countries;
}

/**
 * Save countries.json with array of geonameids
 */
async function saveCountriesList(countries) {
  // Ensure data directory exists
  if (!existsSync(DATA_DIR)) {
    await mkdir(DATA_DIR, { recursive: true });
  }
  
  const geonameids = countries.map(c => c.geonameid);
  const countriesJsonPath = join(DATA_DIR, 'countries.json');
  await writeFile(countriesJsonPath, JSON.stringify(geonameids, null, 2));
  console.log(`✓ Saved countries.json with ${geonameids.length} entries`);
}

/**
 * Create country JSON files
 */
async function createCountryFiles(countries) {
  console.log('Creating country JSON files...');
  
  // Ensure data directory exists
  if (!existsSync(DATA_DIR)) {
    await mkdir(DATA_DIR, { recursive: true });
  }
  
  for (const country of countries) {
    // Create simplified country code file: data/{countrycode}.json
    const countryCodeFile = join(DATA_DIR, `${country.iso}.json`);
    await writeFile(countryCodeFile, JSON.stringify({
      id: country.geonameid,
      name: country.name
    }, null, 2));
    
    // Create full country file: data/{geonameid}.json (children will be added later)
    const countryGeoFile = join(DATA_DIR, `${country.geonameid}.json`);
    await writeFile(countryGeoFile, JSON.stringify({
      id: country.geonameid,
      parent: null, // Countries have no parent
      children: [], // Will be populated when processing ZIP files
      name: country.name,
      level: 0,
      // Additional country info
      iso: country.iso,
      iso3: country.iso3,
      capital: country.capital,
      area: country.area,
      population: country.population,
      continent: country.continent,
      languages: country.languages,
    }, null, 2));
  }
  
  console.log(`✓ Created ${countries.length * 2} country JSON files`);
}

/**
 * Parse a geonames data line (tab-delimited)
 */
function parseGeonamesLine(line) {
  const fields = line.split('\t');
  
  if (fields.length < 19) {
    return null;
  }
  
  return {
    geonameid: fields[0],
    name: fields[1],
    asciiname: fields[2],
    alternatenames: fields[3],
    latitude: fields[4],
    longitude: fields[5],
    featureClass: fields[6],
    featureCode: fields[7],
    countryCode: fields[8],
    cc2: fields[9],
    admin1Code: fields[10],
    admin2Code: fields[11],
    admin3Code: fields[12],
    admin4Code: fields[13],
    population: fields[14],
    elevation: fields[15],
    dem: fields[16],
    timezone: fields[17],
    modificationDate: fields[18],
  };
}

/**
 * Determine the administrative level of a location
 */
function getAdminLevel(location) {
  // Check feature code first
  if (FEATURE_LEVELS[location.featureCode] !== undefined) {
    return FEATURE_LEVELS[location.featureCode];
  }
  
  // For non-ADM locations, determine level by which admin codes they have
  if (location.admin4Code) return 5; // Belongs to ADM4, so it's level 5
  if (location.admin3Code) return 4; // Belongs to ADM3, so it's level 4
  if (location.admin2Code) return 3; // Belongs to ADM2, so it's level 3
  if (location.admin1Code) return 2; // Belongs to ADM1, so it's level 2
  
  return 1; // Belongs to country, level 1
}

/**
 * Determine parent for any location based on admin codes
 */
function determineParentForLocation(location, countryGeonameid, locationsByAdminCode) {
  // If it's an ADM division, use the existing logic
  if (FEATURE_LEVELS[location.featureCode] !== undefined) {
    return determineParent(location, countryGeonameid, locationsByAdminCode);
  }
  
  // For non-ADM locations, find parent based on most specific admin code
  if (location.admin4Code && location.admin3Code && location.admin2Code && location.admin1Code) {
    const key = `${location.countryCode}.${location.admin1Code}.${location.admin2Code}.${location.admin3Code}.${location.admin4Code}`;
    if (locationsByAdminCode[key]) {
      return locationsByAdminCode[key];
    }
  }
  
  if (location.admin3Code && location.admin2Code && location.admin1Code) {
    const key = `${location.countryCode}.${location.admin1Code}.${location.admin2Code}.${location.admin3Code}`;
    if (locationsByAdminCode[key]) {
      return locationsByAdminCode[key];
    }
  }
  
  if (location.admin2Code && location.admin1Code) {
    const key = `${location.countryCode}.${location.admin1Code}.${location.admin2Code}`;
    if (locationsByAdminCode[key]) {
      return locationsByAdminCode[key];
    }
  }
  
  if (location.admin1Code) {
    const key = `${location.countryCode}.${location.admin1Code}`;
    if (locationsByAdminCode[key]) {
      return locationsByAdminCode[key];
    }
  }
  
  // Fallback: country is parent
  return countryGeonameid;
}

/**
 * Determine parent geonameid based on admin codes
 * This requires looking up the parent location in our data
 */
function determineParent(location, countryGeonameid, locationsByAdminCode) {
  const level = getAdminLevel(location);
  
  if (level === 0) {
    return null; // Countries have no parent
  }
  
  if (level === 1) {
    return countryGeonameid; // ADM1 parent is the country
  }
  
  // For ADM2, parent is ADM1
  if (level === 2 && location.admin1Code) {
    const key = `${location.countryCode}.${location.admin1Code}`;
    if (locationsByAdminCode[key]) {
      return locationsByAdminCode[key];
    }
  }
  
  // For ADM3, parent is ADM2
  if (level === 3 && location.admin1Code && location.admin2Code) {
    const key = `${location.countryCode}.${location.admin1Code}.${location.admin2Code}`;
    if (locationsByAdminCode[key]) {
      return locationsByAdminCode[key];
    }
  }
  
  // For ADM4, parent is ADM3
  if (level === 4 && location.admin1Code && location.admin2Code && location.admin3Code) {
    const key = `${location.countryCode}.${location.admin1Code}.${location.admin2Code}.${location.admin3Code}`;
    if (locationsByAdminCode[key]) {
      return locationsByAdminCode[key];
    }
  }
  
  // For ADM5, parent is ADM4
  if (level === 5 && location.admin1Code && location.admin2Code && location.admin3Code && location.admin4Code) {
    const key = `${location.countryCode}.${location.admin1Code}.${location.admin2Code}.${location.admin3Code}.${location.admin4Code}`;
    if (locationsByAdminCode[key]) {
      return locationsByAdminCode[key];
    }
  }
  
  // Fallback: return country as parent
  return countryGeonameid;
}

/**
 * Process a country's ZIP file
 */
async function processCountryZip(country) {
  const zipPath = join(RAW_DIR, `${country.iso}.zip`);
  
  if (!existsSync(zipPath)) {
    console.log(`⚠ Skipping ${country.iso}: ZIP file not found`);
    return;
  }
  
  console.log(`Processing ${country.iso} (${country.name})...`);
  
  try {
    const zip = new AdmZip(zipPath);
    const zipEntries = zip.getEntries();
    
    // Find the main data file (should be {ISO}.txt, not readme.txt)
    const dataEntry = zipEntries.find(entry => 
      entry.entryName === `${country.iso}.txt` || 
      (entry.entryName.endsWith('.txt') && !entry.entryName.includes('readme') && !entry.isDirectory)
    );
    
    if (!dataEntry) {
      console.log(`⚠ No data file found in ${country.iso}.zip`);
      return;
    }
    
    const content = dataEntry.getData().toString('utf8');
    const lines = content.split('\n');
    
    // First pass: build admin code lookup from ADM divisions only
    const locationsByAdminCode = {};
    const allLocations = [];
    
    for (const line of lines) {
      if (!line.trim()) continue;
      
      const location = parseGeonamesLine(line);
      if (!location) continue;
      
      allLocations.push(location);
      
      // Build admin code lookup only for administrative divisions
      if (FEATURE_LEVELS[location.featureCode] !== undefined) {
        const level = getAdminLevel(location);
        
        if (level === 1 && location.admin1Code) {
          const key = `${location.countryCode}.${location.admin1Code}`;
          locationsByAdminCode[key] = location.geonameid;
        } else if (level === 2 && location.admin1Code && location.admin2Code) {
          const key = `${location.countryCode}.${location.admin1Code}.${location.admin2Code}`;
          locationsByAdminCode[key] = location.geonameid;
        } else if (level === 3 && location.admin1Code && location.admin2Code && location.admin3Code) {
          const key = `${location.countryCode}.${location.admin1Code}.${location.admin2Code}.${location.admin3Code}`;
          locationsByAdminCode[key] = location.geonameid;
        } else if (level === 4 && location.admin1Code && location.admin2Code && location.admin3Code && location.admin4Code) {
          const key = `${location.countryCode}.${location.admin1Code}.${location.admin2Code}.${location.admin3Code}.${location.admin4Code}`;
          locationsByAdminCode[key] = location.geonameid;
        }
      }
    }
    
    // Second pass: process ALL locations, determine parents and build child arrays
    const childrenMap = {};
    
    for (const location of allLocations) {
      const level = getAdminLevel(location);
      const parent = determineParentForLocation(location, country.geonameid, locationsByAdminCode);
      
      // Track children
      if (parent) {
        if (!childrenMap[parent]) {
          childrenMap[parent] = [];
        }
        childrenMap[parent].push(location.geonameid);
      }
      
      // Create location JSON file
      const locationFile = join(DATA_DIR, `${location.geonameid}.json`);
      await writeFile(locationFile, JSON.stringify({
        id: location.geonameid,
        parent: parent,
        children: [], // Will be updated below
        name: location.name,
        level: level,
        // Additional useful info
        latitude: location.latitude,
        longitude: location.longitude,
        population: location.population,
      }, null, 2));
    }
    
    // Third pass: update children arrays
    for (const [parentId, childIds] of Object.entries(childrenMap)) {
      const parentFile = join(DATA_DIR, `${parentId}.json`);
      
      if (existsSync(parentFile)) {
        const parentData = JSON.parse(await readFile(parentFile, 'utf-8'));
        parentData.children = childIds;
        await writeFile(parentFile, JSON.stringify(parentData, null, 2));
      }
    }
    
    console.log(`✓ Processed ${allLocations.length} locations from ${country.iso}`);
    
  } catch (error) {
    console.error(`✗ Error processing ${country.iso}:`, error.message);
  }
}

/**
 * Main execution
 */
async function main() {
  console.log('Starting transformation...\n');
  
  try {
    // Step 1: Parse country info
    const countries = await parseCountryInfo();
    
    // Step 2: Save countries.json
    await saveCountriesList(countries);
    
    // Step 3: Create country JSON files
    await createCountryFiles(countries);
    
    // Step 4: Process each country's ZIP file
    console.log('\nProcessing country data files...');
    for (const country of countries) {
      await processCountryZip(country);
    }
    
    console.log('\n✓ Transformation complete!');
    
  } catch (error) {
    console.error('✗ Transformation failed:', error);
    process.exit(1);
  }
}

// Run the transformation
main();
