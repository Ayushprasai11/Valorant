-- Select top 5 players by kills
SELECT Event, player, kills , deaths, Assists, kda , maps
FROM `valorant stats`
ORDER BY kills DESC
LIMIT 5;

-- Create a new table with aggregated player stats
CREATE TABLE PlayerAggregatedStats AS
SELECT
    Player,
    SUM(Maps) AS Total_Maps,                   -- Summing the total number of maps played by each player
    SUM(Kills) AS Total_Kills,                 -- Summing the total number of kills by each player
    SUM(Deaths) AS Total_Deaths,               -- Summing the total number of deaths by each player
    SUM(Assists) AS Total_Assists,             -- Summing the total number of assists by each player
    SUM(ACS_Map * Maps) / SUM(Maps) AS Average_ACS  -- Calculating average ACS (Average Combat Score) per map
FROM `valorant stats`
GROUP BY Player;  -- Grouping the results by player

-- Add a new column to store Overall K/D ratio
ALTER TABLE PlayerAggregatedStats
ADD COLUMN Overall_KD FLOAT;

-- Ensuring safe updates are enabled
SET SQL_SAFE_UPDATES = 1;

-- Update the Overall_KD column with the ratio of Total_Kills to Total_Deaths
UPDATE PlayerAggregatedStats
SET Overall_KD = Total_Kills / Total_Deaths;

-- Select top 10 players by Overall K/D ratio
SELECT Player, Overall_KD
FROM PlayerAggregatedStats
ORDER BY Overall_KD DESC
LIMIT 10;

-- Select top 10 players by Total Kills
SELECT Player, Total_Maps, Total_Kills, Total_Deaths, Total_Assists
FROM PlayerAggregatedStats
ORDER BY Total_Kills DESC
LIMIT 10;

-- Select top 10 players by Average ACS, who have played more than 10 maps
SELECT Player, Total_Maps, Total_Kills, Total_Deaths, Total_Assists, Average_ACS
FROM PlayerAggregatedStats
HAVING Total_Maps > 10
ORDER BY Average_ACS DESC
LIMIT 10;

-- Select top 10 player-event combinations by ACS per map, for players who have played more than 10 maps
SELECT Event, Player, maps, kills, deaths, ACS_Map
FROM `valorant stats`
HAVING maps > 10
ORDER BY ACS_Map DESC
LIMIT 10;
