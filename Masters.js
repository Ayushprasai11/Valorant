const puppeteer = require('puppeteer');
const { MongoClient } = require('mongodb');

class Master {
    constructor(mongoUrl, dbName, collectionName) {
        this.mongoUrl = mongoUrl; // MongoDB connection URL
        this.dbName = dbName; // Database name in MongoDB
        this.collectionName = collectionName; // Collection name in the database
        this.configs = {}; // Object to store different scraping configurations
    }

    // Method to add a new scraping configuration
    addConfig(name, config) {
        this.configs[name] = config;
    }

    // Method to scrape event data using a specified configuration
    async scrapeEventData(url, config, eventName) {
        const browser = await puppeteer.launch({ headless: false });
        const page = await browser.newPage();

        await page.goto(url, { waitUntil: 'networkidle2' });

        await page.waitForSelector(config.tableSelector);

        const tableData = await page.evaluate((config, eventName) => {
            const results = [];
            const table = document.querySelector(config.tableSelector);
            const headers = Array.from(table.querySelectorAll(config.headerSelector)).map(header => header.innerText.trim());

            // Debugging: Print headers
            console.log('Headers:', headers);

            const rows = table.querySelectorAll(config.rowSelector);
            rows.forEach(row => {
                const cells = Array.from(row.querySelectorAll(config.cellSelector)).map(cell => cell.innerText.trim());
                
                // Debugging: Print each row's cells
                console.log('Row cells:', cells);

                const rowData = {};
                cells.forEach((cell, index) => {
                    rowData[headers[index]] = cell;
                });
                results.push(rowData);
            });

            return results.map(row => {
                const mappedRow = { Event: eventName }; // Add the Event column
                for (const key in config.mappings) {
                    mappedRow[key] = row[config.mappings[key]] || 'N/A';
                }
                return mappedRow;
            });
        }, config, eventName);

        await browser.close();
        return tableData;
    }

    // Method to insert scraped data into MongoDB
    async insertDataToDB(data) {
        const client = new MongoClient(this.mongoUrl);
        try {
            await client.connect();
            console.log('Connected successfully to server');

            const db = client.db(this.dbName);
            const collection = db.collection(this.collectionName);

            const insertResults = await collection.insertMany(data);
            console.log('Data inserted successfully:', insertResults.insertedIds);
        } finally {
            await client.close();
        }
    }

    // Method to run the scraper with retry logic
    async run(urlsAndConfigs) {
        const maxAttempts = 5;
        let attempts = 0;
        let allData = [];

        for (const { url, configName, eventName } of urlsAndConfigs) {
            const config = this.configs[configName];

            if (!config) {
                console.error(`No configuration found for ${configName}`);
                continue;
            }

            while (attempts < maxAttempts) {
                try {
                    const data = await this.scrapeEventData(url, config, eventName);
                    
                    // Log the data before inserting into the database
                    console.log(`Scraped Data for ${configName}:`, JSON.stringify(data, null, 2));
                    
                    allData = allData.concat(data);
                    break;
                } catch (error) {
                    console.error('Error:', error);
                    attempts++;
                    if (attempts < maxAttempts) {
                        console.log(`Retrying to run... ${maxAttempts - attempts} attempts left`);
                        await new Promise(resolve => setTimeout(resolve, 5000)); // Wait 5 seconds before retrying
                    } else {
                        console.log('Max attempts reached. Exiting.');
                    }
                }
            }
        }

        if (allData.length > 0) {
            await this.insertDataToDB(allData);
        }
    }
}

// Example usage:
(async () => {
    const master = new Master(
        'mongodb://localhost:27017', // # MongoDB connection URL
        'game_stats', // # Database name
        'player_stats' // # Collection name
    );

    // Add configurations for different events
    master.addConfig('valorantMastersMadrid', {
        tableSelector: 'div.table-responsive', // CSS selector for the table
        headerSelector: 'thead th', // CSS selector for the table headers
        rowSelector: 'tbody tr', // CSS selector for the table rows
        cellSelector: 'td', // CSS selector for the table cells
        mappings: { // Mapping of table columns to desired output keys
            Rank: '#', // Column for rank
            Player: 'Player', // Column for player name
            Maps: 'Maps', // Column for number of maps
            Kills: 'K', // Column for kills
            Deaths: 'D', // Column for deaths
            Assists: 'A', // Column for assists
            KD: 'KD', // Column for KD ratio
            KDA: 'KDA', // Column for KDA ratio
            ACS_Map: 'ACS/Map', // Column for ACS per map
            K_Map: 'K/Map', // Column for kills per map
            D_Map: 'D/Map', // Column for deaths per map
            A_Map: 'A/Map' // Column for assists per map
        }
    });

    master.addConfig('valorantMasterShangai', {
        tableSelector: 'div.table-responsive', // CSS selector for the table
        headerSelector: 'thead th', // CSS selector for the table headers
        rowSelector: 'tbody tr', // CSS selector for the table rows
        cellSelector: 'td', // CSS selector for the table cells
        mappings: { // Mapping of table columns to desired output keys
            Rank: '#', // Column for rank
            Player: 'Player', // Column for player name
            Maps: 'Maps', // Column for number of maps
            Kills: 'K', // Column for kills
            Deaths: 'D', // Column for deaths
            Assists: 'A', // Column for assists
            KD: 'KD', // Column for KD ratio
            KDA: 'KDA', // Column for KDA ratio
            ACS_Map: 'ACS/Map', // Column for ACS per map
            K_Map: 'K/Map', // Column for kills per map
            D_Map: 'D/Map', // Column for deaths per map
            A_Map: 'A/Map' // Column for assists per map
        }
    });

    master.addConfig('valorantChampion2023', {
        tableSelector: 'div.table-responsive', // CSS selector for the table
        headerSelector: 'thead th', // CSS selector for the table headers
        rowSelector: 'tbody tr', // CSS selector for the table rows
        cellSelector: 'td', // CSS selector for the table cells
        mappings: { // Mapping of table columns to desired output keys
            Rank: '#', // Column for rank
            Player: 'Player', // Column for player name
            Maps: 'Maps', // Column for number of maps
            Kills: 'K', // Column for kills
            Deaths: 'D', // Column for deaths
            Assists: 'A', // Column for assists
            KD: 'KD', // Column for KD ratio
            KDA: 'KDA', // Column for KDA ratio
            ACS_Map: 'ACS/Map', // Column for ACS per map
            K_Map: 'K/Map', // Column for kills per map
            D_Map: 'D/Map', // Column for deaths per map
            A_Map: 'A/Map' // Column for assists per map
        }
    });

    master.addConfig('valorantMastersTokyo', {
        tableSelector: 'div.table-responsive', // CSS selector for the table
        headerSelector: 'thead th', // CSS selector for the table headers
        rowSelector: 'tbody tr', // CSS selector for the table rows
        cellSelector: 'td', // CSS selector for the table cells
        mappings: { // Mapping of table columns to desired output keys
            Rank: '#', // Column for rank
            Player: 'Player', // Column for player name
            Maps: 'Maps', // Column for number of maps
            Kills: 'K', // Column for kills
            Deaths: 'D', // Column for deaths
            Assists: 'A', // Column for assists
            KD: 'KD', // Column for KD ratio
            KDA: 'KDA', // Column for KDA ratio
            ACS_Map: 'ACS/Map', // Column for ACS per map
            K_Map: 'K/Map', // Column for kills per map
            D_Map: 'D/Map', // Column for deaths per map
            A_Map: 'A/Map' // Column for assists per map
        }
    });

    // Scrape data from multiple events
    const urlsAndConfigs = [
        { url: 'https://liquipedia.net/valorant/VCT/2024/Stage_1/Masters/Statistics', configName: 'valorantMastersMadrid', eventName: 'Masters Madrid' },
        { url: 'https://liquipedia.net/valorant/VCT/2024/Stage_2/Masters/Statistics', configName: 'valorantMasterShangai', eventName: 'Masters Shangai' },
        { url: 'https://liquipedia.net/valorant/VCT/2023/Champions/Statistics', configName: 'valorantChampion2023', eventName: 'Champions 2023' },
        { url: 'https://liquipedia.net/valorant/VCT/2023/Masters/Statistics', configName: 'valorantMastersTokyo', eventName: 'Masters Tokyo' }
    ];

    await master.run(urlsAndConfigs);
})();
