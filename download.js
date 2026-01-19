// This file downloads data from geonames.org and saves it to a local file.
// First, it downloads the list of countries, then for each country, it downloads the corresponding country data.
// It saves the downloaded data to ./raw/ with their respective original filenames.

import { createWriteStream } from 'fs';
import { mkdir } from 'fs/promises';
import { dirname, join } from 'path';
import { fileURLToPath } from 'url';
import { pipeline } from 'stream/promises';

const __dirname = dirname(fileURLToPath(import.meta.url));
const RAW_DIR = join(__dirname, 'raw');

// geonames.org URLs
const COUNTRY_INFO_URL = 'http://download.geonames.org/export/dump/countryInfo.txt';
const GEONAMES_DUMP_BASE = 'http://download.geonames.org/export/dump/';

/**
 * Download a file from a URL and save it to disk
 */
async function downloadFile(url, filepath) {
  console.log(`Downloading ${url}...`);
  
  try {
    const response = await fetch(url);
    
    if (!response.ok) {
      throw new Error(`HTTP error! status: ${response.status}`);
    }
    
    const fileStream = createWriteStream(filepath);
    await pipeline(response.body, fileStream);
    
    console.log(`✓ Saved to ${filepath}`);
  } catch (error) {
    console.error(`✗ Failed to download ${url}:`, error.message);
    throw error;
  }
}

/**
 * Parse countryInfo.txt to extract country codes
 */
function parseCountryCodes(countryInfoContent) {
  const lines = countryInfoContent.split('\n');
  const countryCodes = [];
  
  for (const line of lines) {
    // Skip comments and empty lines
    if (line.startsWith('#') || line.trim() === '') {
      continue;
    }
    
    // Extract country code (first column)
    const columns = line.split('\t');
    const countryCode = columns[0];
    
    if (countryCode) {
      countryCodes.push(countryCode);
    }
  }
  
  return countryCodes;
}

/**
 * Main download function
 */
async function main() {
  try {
    // Ensure raw directory exists
    await mkdir(RAW_DIR, { recursive: true });
    console.log(`Output directory: ${RAW_DIR}\n`);
    
    // Step 1: Download country info file
    const countryInfoPath = join(RAW_DIR, 'countryInfo.txt');
    await downloadFile(COUNTRY_INFO_URL, countryInfoPath);
    
    // Read and parse country codes
    const countryInfoResponse = await fetch(COUNTRY_INFO_URL);
    const countryInfoContent = await countryInfoResponse.text();
    const countryCodes = parseCountryCodes(countryInfoContent);
    
    console.log(`\nFound ${countryCodes.length} countries\n`);
    
    // Step 2: Download each country's data file
    for (let i = 0; i < countryCodes.length; i++) {
      const countryCode = countryCodes[i];
      const filename = `${countryCode}.zip`;
      const url = `${GEONAMES_DUMP_BASE}${filename}`;
      const filepath = join(RAW_DIR, filename);
      
      console.log(`[${i + 1}/${countryCodes.length}] ${countryCode}`);
      
      try {
        await downloadFile(url, filepath);
      } catch (error) {
        console.error(`Skipping ${countryCode} due to error\n`);
        continue;
      }
      
      // Small delay to be respectful to the server
      await new Promise(resolve => setTimeout(resolve, 100));
    }
    
    console.log('\n✓ Download complete!');
  } catch (error) {
    console.error('Fatal error:', error);
    process.exit(1);
  }
}

// Run the script
main();