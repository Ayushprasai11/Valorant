const puppeteer = require('puppeteer');
const { MongoClient } = require('mongodb');

(async () => {
    let browser;

    // Launch the browser
    browser = await puppeteer.launch({ headless: false });
    const page = await browser.newPage();

    // Go to the specific match page
    await page.goto('https://liquipedia.net/valorant/VCT/2024/Stage_1/Masters/Statistics', { waitUntil: 'networkidle2' });

    // Extract the table data
    await page.waitForSelector('div.table-responsive');

    const tableData = await page.evaluate(() => {
        const results = [];
        const table = document.querySelector('div.table-responsive');
        const headers = Array.from(table.querySelectorAll('thead th')).map(header => header.innerText.trim());

        const rows = table.querySelectorAll('tbody tr');
        rows.forEach(row => {
            const cells = Array.from(row.querySelectorAll('td')).map(cell => cell.innerText.trim());
            const rowData = {};
            cells.forEach((cell, index) => {
                rowData[headers[index]] = cell;
            });
            results.push(rowData);
        });

        // Filter out unnecessary columns and map to the desired schema
        return results.map(row => ({
            Rank: row['#'] || 'N/A',
            Player: row['Player'] || 'N/A',
            Maps: row['Maps'] || 'N/A',
            Kills: row['K'] || 'N/A',
            Deaths: row['D'] || 'N/A',
            Assists: row['A'] || 'N/A',
            KD: row['KD'] || 'N/A',
            KDA: row['KDA'] || 'N/A',
            ACS_Map: row['ACS/Map'] || 'N/A',
            K_Map: row['K/Map'] || 'N/A',
            D_Map: row['D/Map'] || 'N/A',
            A_Map: row['A/Map'] || 'N/A'
        }));
    });

    console.log(tableData);

    // MongoDB setup
    const url = 'mongodb://localhost:27017';
    const client = new MongoClient(url);
    const dbName = 'game_stats';
    const collectionName = 'player_stats';

    async function run() {
        try {
            // Connect to MongoDB server
            await client.connect();
            console.log('Connected successfully to server');

            const db = client.db(dbName);
            const collection = db.collection(collectionName);

            // Insert each player's stats from tableData
            const insertResults = await collection.insertMany(tableData);
            console.log('Data inserted successfully:', insertResults.insertedIds);

        } finally {
            // Close the connection
            await client.close();
        }
    }

    // Loop to retry the run function
    const maxAttempts = 5;
    let attempts = 0;
    while (attempts < maxAttempts) {
        try {
            await run();
            break; // Break the loop if run() is successful
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

    await browser.close();
})();
